#[derive(Debug, Default)]
pub struct ReplicationManager;

impl ReplicationManager {
    pub fn new() -> Self {
        Self
    }

    pub fn is_active(&self) -> bool {
        false
    }

    pub fn queue_len(&self) -> usize {
        0
    }

    pub fn tick(&mut self) {}
}