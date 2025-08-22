use nayud_batch::replication::{FailoverManager, Cluster};
use nayud_batch::db::DbClients;

#[ntex::test]
async fn last_status_updates_and_no_switch_when_both_down() {
    let clients = DbClients::default();
    let mut fm = FailoverManager::new();

    let _ = fm.tick_with_status(&clients, false, false).await;
    assert_eq!(fm.last_status(), (false, false));
    assert_eq!(fm.current_primary(), Cluster::Active);

    let _ = fm.tick_with_status(&clients, true, false).await;
    assert_eq!(fm.last_status(), (true, false));
    assert_eq!(fm.current_primary(), Cluster::Active);
}