impl Service {
    async fn set_marker(&self, value: &str) -> Result<(), sqlx::Error> {
        sqlx::query(AssertSqlSafe(format!("update {TABLE}_lock set marker = marker + 1 where v = ?")))
            .bind(value.to_owned())
            .execute(&self.pool)
            .await
            .map(|_| ())
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
        assert_eq!(error.as_database_error().and_then(|database| database.code()).as_deref(), Some("40001"), "{error}");
        let after = self.insert("after-deadlock").await;
        let refused = after.as_ref().err().and_then(ServiceError::infrastructure);
        assert!(matches!(refused, Some(TransactionError::InvalidState(_))), "{after:?}");
        Ok(true)
    }
}

db_test!(after_a_deadlock_the_connection_refuses_statements_and_the_transaction_cannot_commit, |service| {
    let raw = service.pool.inner();
    sqlx::query(AssertSqlSafe(format!("drop table if exists {TABLE}_lock"))).execute(raw).await.unwrap();
    sqlx::query(AssertSqlSafe(format!("create table {TABLE}_lock (v varchar(100) primary key, marker int not null)")))
        .execute(raw)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("insert into {TABLE}_lock values ('a', 0), ('b', 0)"))).execute(raw).await.unwrap();

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
    let markers: Vec<i32> = sqlx::query_scalar(AssertSqlSafe(format!("select marker from {TABLE}_lock order by v")))
        .fetch_all(raw)
        .await
        .unwrap();
    assert_eq!(markers, [1, 1], "only the surviving transaction changed the rows");
});
