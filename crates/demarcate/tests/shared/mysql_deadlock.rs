impl Service {
    async fn set_marker(&self, value: &str) -> Result<(), db::Error> {
        db::increment_marker(&self.pool, &format!("{TABLE}_lock"), value).await
    }

    // Locks one row, waits until the other call has locked the other row, then asks for that row.
    // The server ends one of the two calls with a deadlock error. That call ignores the error and
    // goes on.
    #[transactional]
    async fn lock_both(&self, first: &str, second: &str, both_locked: Arc<tokio::sync::Barrier>) -> Result<bool, ServiceError> {
        self.set_marker(first).await?;
        both_locked.wait().await;
        let Err(error) = self.set_marker(second).await else {
            return Ok(false);
        };
        assert_eq!(db::sqlstate(&error).as_deref(), Some("40001"), "{error}");
        let after = self.insert("after-deadlock").await;
        let refused = after.as_ref().err().and_then(ServiceError::infrastructure);
        assert!(matches!(refused, Some(TransactionError::InvalidState(_))), "{after:?}");
        Ok(true)
    }
}

impl Service {
    // Reads a row, lets another connection change it, and then updates it. With
    // innodb_snapshot_isolation, MariaDB ends the transaction with error 1020 at the update.
    #[transactional(isolation = RepeatableRead)]
    async fn update_a_row_that_changed(&self) -> Result<bool, ServiceError> {
        self.insert("before-the-update").await?;
        db::values(&self.pool, &format!("{TABLE}_lock")).await?;
        db::raw_execute(&self.raw, &format!("update {TABLE}_lock set marker = marker + 1 where v = 'a'")).await;
        if self.set_marker("a").await.is_ok() {
            return Ok(false);
        }
        let after = self.insert("after-the-update").await;
        let refused = after.as_ref().err().and_then(ServiceError::infrastructure);
        assert!(matches!(refused, Some(TransactionError::InvalidState(_))), "{after:?}");
        Ok(true)
    }
}

db_test!(a_changed_row_under_snapshot_isolation, |service| {
    let raw = &service.raw;
    db::raw_execute(raw, &format!("drop table if exists {TABLE}_lock")).await;
    db::raw_execute(raw, &format!("create table {TABLE}_lock (v varchar(100) primary key, marker int not null)")).await;
    db::raw_execute(raw, &format!("insert into {TABLE}_lock values ('a', 0)")).await;
    let version = db::raw_values(raw, "select version() from dual").await;
    let snapshot_isolation = version[0].contains("MariaDB")
        && db::raw_values(raw, "select if(@@innodb_snapshot_isolation, 'on', 'off') from dual").await == ["on"];

    let result = service.update_a_row_that_changed().await;

    let markers = db::raw_values(raw, &format!("select cast(marker as char) from {TABLE}_lock order by v")).await;
    if snapshot_isolation {
        assert!(
            matches!(result, Err(ServiceError::Transaction(TransactionError::UnexpectedRollback(_)))),
            "the server ended the transaction, so it cannot commit: {result:?}"
        );
        assert!(service.committed().await.is_empty(), "nothing ran outside the transaction after the error");
        assert_eq!(markers, ["1"]);
    } else {
        assert!(matches!(result, Ok(false)), "{result:?}");
        assert_eq!(service.committed().await, ["before-the-update"]);
        assert_eq!(markers, ["2"]);
    }
});

db_test!(after_a_deadlock_the_connection_refuses_statements_and_the_transaction_cannot_commit, |service| {
    let raw = &service.raw;
    db::raw_execute(raw, &format!("drop table if exists {TABLE}_lock")).await;
    db::raw_execute(raw, &format!("create table {TABLE}_lock (v varchar(100) primary key, marker int not null)")).await;
    db::raw_execute(raw, &format!("insert into {TABLE}_lock values ('a', 0), ('b', 0)")).await;

    let both_locked = Arc::new(tokio::sync::Barrier::new(2));
    let (forward, backward) =
        tokio::join!(service.lock_both("a", "b", both_locked.clone()), service.lock_both("b", "a", both_locked));

    let (victim, survivor) = match (&forward, &backward) {
        (Err(_), Ok(_)) => (&forward, &backward),
        (Ok(_), Err(_)) => (&backward, &forward),
        other => panic!("exactly one call must lose its transaction: {other:?}"),
    };
    assert!(
        matches!(victim, Err(ServiceError::Transaction(TransactionError::UnexpectedRollback(_)))),
        "the call that ignored the deadlock cannot commit: {victim:?}"
    );
    assert!(matches!(survivor, Ok(false)), "{survivor:?}");
    assert!(service.committed().await.is_empty(), "nothing ran outside the transaction after the deadlock");
    let markers = db::raw_values(raw, &format!("select cast(marker as char) from {TABLE}_lock order by v")).await;
    assert_eq!(markers, ["1", "1"], "only the surviving transaction changed the rows");
});
