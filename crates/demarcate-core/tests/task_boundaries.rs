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

fn assert_concurrent<T: std::fmt::Debug>(result: Result<T, TransactionError>) {
    match result {
        Err(TransactionError::InvalidState(message)) => {
            assert!(message.contains("already running on this task"), "{message}");
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[tokio::test]
async fn concurrent_call_on_one_task_is_rejected_at_entry() {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = &manager(ds);
        let tt = &template(tm, TransactionDefinition::new());

        let first = execute(tt, |status| async move {
            assert!(status.is_new_transaction());
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(has_resource(ds));
            Ok(())
        });
        let second = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert!(!has_resource(ds), "a sibling future does not see the other call's transaction");
            assert!(!tsm::is_actual_transaction_active());
            execute(tt, |_status| async move { Ok(()) }).await
        };
        let (first, second) = tokio::join!(first, second);

        first.unwrap();
        assert_concurrent(second);
        con.log.verify("commit");
        con.log.verify_times("rollback", 0);
        assert!(!has_resource(ds));
    })
    .await;
}

#[tokio::test]
async fn concurrent_requires_new_that_would_finish_first_is_rejected() {
    scoped(async {
        let con1 = Con::new();
        let con2 = Con::new();
        let ds = &MockDataSource::returning(&[&con1, &con2]);
        let tm = &manager(ds);
        let tt = &template(tm, def(Propagation::RequiresNew));

        let slow = execute(tt, |_status| async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            Ok(())
        });
        let fast = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            execute(tt, |_status| async move { Ok(()) }).await
        };
        let (slow, fast) = tokio::join!(slow, fast);

        slow.unwrap();
        assert_concurrent(fast);
        con1.log.verify("commit");
        con2.log.verify_no_interactions();
    })
    .await;
}

#[tokio::test]
async fn concurrent_requires_new_inside_a_transaction_keeps_the_outer_transaction() {
    scoped(async {
        let con1 = &Con::new();
        let con2 = &Con::new();
        let ds = &MockDataSource::returning(&[con1, con2]);
        let tm = &manager(ds);
        let outer = template(tm, TransactionDefinition::new());

        execute(&outer, |_status| async move {
            let inner = &template(tm, def(Propagation::RequiresNew));
            let first = execute(inner, |_inner| async move {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Ok(())
            });
            let second = async {
                tokio::time::sleep(Duration::from_millis(5)).await;
                execute(inner, |_inner| async move { Ok(()) }).await
            };
            let (first, second) = tokio::join!(first, second);
            first?;
            assert_concurrent(second);
            assert!(con1.is(&utils::connection(ds).await?), "the outer transaction is bound again");
            assert!(tsm::is_actual_transaction_active());
            Ok(())
        })
        .await
        .unwrap();
        con1.log.verify("commit");
        con2.log.verify("commit");
    })
    .await;
}

#[tokio::test]
async fn select_loser_does_not_break_the_winner() {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = &manager(ds);
        let tt = &template(tm, TransactionDefinition::new());

        let winner = tokio::select! {
            result = execute(tt, |_status| async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok("transaction")
            }) => result,
            _ = tokio::time::sleep(Duration::from_secs(5)) => Ok("timer"),
        };
        assert_eq!(winner.unwrap(), "transaction");
        con.log.verify("commit");
        assert!(!has_resource(ds));
    })
    .await;
}

#[tokio::test]
async fn binding_over_an_existing_resource_keeps_the_existing_one() {
    tsm::scope(async {
        let key = tsm::ResourceKey::named("resource");
        tsm::bind_resource(key.clone(), tsm::BoundResource::value(std::sync::Arc::new(1_u32))).unwrap();
        assert!(tsm::bind_resource(key.clone(), tsm::BoundResource::value(std::sync::Arc::new(2_u32))).is_err());
        let bound = tsm::resource(&key).unwrap().downcast::<u32>().unwrap();
        assert_eq!(*bound, 1);
        tsm::unbind_resource(&key).unwrap();
    })
    .await;
}
