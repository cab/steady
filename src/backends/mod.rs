pub(crate) mod memory;
pub(crate) mod redis;

use crate::{jobs::JobDefinition, scheduler::QueueName, Result};
use std::num::NonZeroUsize;

#[async_trait::async_trait]
pub trait Backend: Clone + Send + Sync {
    async fn schedule(&self, job_def: &JobDefinition) -> Result<()>;
    async fn pull(&self, queue: &QueueName, count: NonZeroUsize) -> Result<Vec<JobDefinition>>;
}
