pub(crate) mod memory;
#[cfg(feature = "backend-redis")]
pub(crate) mod redis;

use chrono::{DateTime, Utc};

use crate::{jobs::JobDefinition, QueueName, Result};
use std::num::NonZeroUsize;

#[async_trait::async_trait]
pub trait Backend: Clone + Send + Sync {
    async fn enqueue(&self, job_def: &JobDefinition, perform_at: DateTime<Utc>) -> Result<()>;
    async fn pull(&self, queue: &QueueName, count: NonZeroUsize) -> Result<Vec<JobDefinition>>;
    async fn pull_scheduled(&self, count: NonZeroUsize) -> Result<Vec<JobDefinition>>;
}

#[async_trait::async_trait]
pub trait CronBackend: Clone + Send + Sync {
    async fn schedule(
        &self,
        job_def: &JobDefinition,
        perform_at: chrono::DateTime<Utc>,
    ) -> Result<()>;
}
