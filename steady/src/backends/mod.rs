pub(crate) mod memory;
#[cfg(feature = "backend-redis")]
pub(crate) mod redis;

use crate::{jobs::JobDefinition, QueueName, Result};
use std::num::NonZeroUsize;

#[async_trait::async_trait]
pub trait Backend: Clone + Send + Sync {
    async fn enqueue(&self, job_def: &JobDefinition) -> Result<()>;
    async fn pull(&self, queue: &QueueName, count: NonZeroUsize) -> Result<Vec<JobDefinition>>;
}
