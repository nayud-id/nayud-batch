use nayud_batch::replication::{ReplicationManager, OutboxRecord, OutboxTarget};
use std::path::PathBuf;
use std::fs;

fn temp_outbox_dir() -> PathBuf {
    let mut dir = std::env::temp_dir();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    dir.push(format!("nayud_batch_test_outbox_{}", ts));
    dir
}

#[ntex::test]
async fn outbox_enqueue_and_replay_marks_cursor() {
    let dir = temp_outbox_dir();
    let _ = fs::remove_dir_all(&dir);

    let mut rm = ReplicationManager::with_outbox_dir(&dir).expect("open outbox");
    assert_eq!(rm.queue_len(), 0);

    let _ = rm.enqueue(OutboxRecord::new_simple("k1", "INSERT INTO t ...", OutboxTarget::Active)).unwrap();
    let _ = rm.enqueue(OutboxRecord::new_simple("k2", "INSERT INTO t ...", OutboxTarget::Passive)).unwrap();

    assert_eq!(rm.queue_len(), 2);

    let processed = rm.replay_with(1, |_rec| async move { true }).await.unwrap();
    assert_eq!(processed, 1);
    assert_eq!(rm.queue_len(), 1);

    let processed2 = rm.replay_with(10, |_rec| async move { true }).await.unwrap();
    assert_eq!(processed2, 1);
    assert_eq!(rm.queue_len(), 0);

    let _ = fs::remove_dir_all(&dir);
}