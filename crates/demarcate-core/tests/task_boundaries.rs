mod support;

use std::time::Duration;

use demarcate_core::{Propagation, TransactionDefinition, TransactionError, synchronization as tsm};
use support::*;

fn def(propagation: Propagation) -> TransactionDefinition {
    TransactionDefinition::with_propagation(propagation)
}

#[tokio::test]
async fn dropped_transaction_releases_the_task_state() {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = manager(ds);
        let tt = &template(&tm, TransactionDefinition::new());

        let cancelled = tokio::time::timeout(
            Duration::from_millis(50),
            execute(tt, |_status| async move {
                assert!(has_resource(ds));
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(())
            }),
        )
        .await;
        assert!(cancelled.is_err());
        assert!(!has_resource(ds));
        assert!(!tsm::is_synchronization_active());
        assert!(!tsm::is_actual_transaction_active());

        execute(tt, |status| async move {
            assert!(status.is_new_transaction());
            Ok(())
        })
        .await
        .unwrap();
        con.log.verify("commit");
    })
    .await;
}

#[tokio::test]
async fn dropped_requires_new_resumes_the_outer_transaction() {
    scoped(async {
        let con1 = &Con::new();
        let con2 = &Con::new();
        let ds = &MockDataSource::returning(&[con1, con2]);
        let tm = &manager(ds);
        let outer = template(tm, TransactionDefinition::new());

        execute(&outer, |status| async move {
            let inner = template(tm, def(Propagation::RequiresNew));
            let cancelled = tokio::time::timeout(
                Duration::from_millis(50),
                execute(&inner, |inner_status| async move {
                    assert!(inner_status.is_new_transaction());
                    assert!(con2.is(&utils::connection(ds).await?));
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    Ok(())
                }),
            )
            .await;
            assert!(cancelled.is_err());
            assert!(has_resource(ds));
            assert!(tsm::is_synchronization_active());
            assert!(tsm::is_actual_transaction_active());
            assert!(con1.is(&utils::connection(ds).await?));
            assert!(status.is_new_transaction());
            Ok(())
        })
        .await
        .unwrap();
        con1.log.verify("commit");
        con2.log.verify_times("commit", 0);
    })
    .await;
}

#[tokio::test]
async fn dropped_participation_marks_the_outer_transaction_rollback_only() {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = &manager(ds);
        let tt = &template(tm, TransactionDefinition::new());

        let error = execute(tt, |status| async move {
            let cancelled = tokio::time::timeout(
                Duration::from_millis(50),
                execute(tt, |_inner| async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    Ok(())
                }),
            )
            .await;
            assert!(cancelled.is_err());
            assert!(status.is_rollback_only());
            Ok(())
        })
        .await
        .unwrap_err();
        assert!(matches!(error, TransactionError::UnexpectedRollback(_)), "{error:?}");
        con.log.verify("rollback");
        con.log.verify_times("commit", 0);
    })
    .await;
}

#[tokio::test]
async fn transaction_template_with_error() {
    let con = Con::new();
    let ds = MockDataSource::returning(&[&con]);
    let tm = manager(&ds);
    let joined = tsm::spawn(async move {
        let tt = template(&tm, TransactionDefinition::new());
        execute(&tt, |_status| async move {
            if true {
                panic!("Some application error");
            }
            Ok(())
        })
        .await
    })
    .await;
    assert!(joined.unwrap_err().is_panic());
    con.log.verify("rollback");
    con.log.verify_times("commit", 0);
    con.log.verify("close");
}

#[tokio::test]
async fn concurrent_boundaries_on_one_task_fail_loudly() {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = &manager(ds);
        let tt = &template(tm, TransactionDefinition::new());

        let first = execute(tt, |_status| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(())
        });
        let second = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            execute(tt, |status| async move {
                assert!(!status.is_new_transaction());
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(())
            })
            .await
        };
        let (first, second) = tokio::join!(first, second);

        for result in [first, second] {
            match result {
                Err(TransactionError::InvalidState(message)) => {
                    assert!(message.contains("out of order"), "{message}");
                }
                other => panic!("unexpected result: {other:?}"),
            }
        }
        con.log.verify_times("commit", 0);
        con.log.verify("rollback");
        assert!(!has_resource(ds));
    })
    .await;
}
