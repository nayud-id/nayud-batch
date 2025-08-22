use nayud_batch::replication::{FailoverManager, Cluster};
use nayud_batch::db::DbClients;

#[ntex::test]
async fn failover_to_passive_after_consecutive_failures() {
    let clients = DbClients::default();
    let mut fm = FailoverManager::new().with_force_ready(true);

    for _ in 0..3 {
        let _ = fm.tick_with_status(&clients, false, true).await;
    }

    assert_eq!(fm.current_primary(), Cluster::Passive, "primary should switch to Passive after threshold and readiness");
    assert!(fm.last_switch().is_some(), "last_switch should be recorded");
}

#[ntex::test]
async fn no_recover_before_threshold() {
    let clients = DbClients::default();
    let mut fm = FailoverManager::new().with_force_ready(true);

    for _ in 0..3 { let _ = fm.tick_with_status(&clients, false, true).await; }
    assert_eq!(fm.current_primary(), Cluster::Passive);

    for _ in 0..4 { let _ = fm.tick_with_status(&clients, true, true).await; }
    assert_eq!(fm.current_primary(), Cluster::Passive, "should not recover to Active before reaching threshold");
}

#[ntex::test]
async fn recover_to_active_after_threshold() {
    let clients = DbClients::default();
    let mut fm = FailoverManager::new().with_force_ready(true);

    for _ in 0..3 { let _ = fm.tick_with_status(&clients, false, true).await; }
    assert_eq!(fm.current_primary(), Cluster::Passive);

    for _ in 0..5 { let _ = fm.tick_with_status(&clients, true, true).await; }
    assert_eq!(fm.current_primary(), Cluster::Active, "should recover to Active after threshold and readiness");
}

#[ntex::test]
async fn failover_blocked_without_readiness() {
    let clients = DbClients::default();
    let mut fm = FailoverManager::new();

    for _ in 0..3 {
        let _ = fm.tick_with_status(&clients, false, true).await;
    }

    assert_eq!(fm.current_primary(), Cluster::Active, "should not switch without readiness");
}