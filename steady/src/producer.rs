use crate::{
    backends,
    error::Result,
    jobs::{self, JobDefinition, QueueName},
    JobHandler,
};
use chrono::{DateTime, Utc};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{debug, instrument};

#[derive(Clone)]
pub struct Producer<Backend> {
    backend: Backend,
}

impl<Backend> Producer<Backend>
where
    Backend: backends::Backend + 'static,
{
    pub fn new(backend: Backend) -> Self {
        Self { backend }
    }

    #[instrument(skip(self))]
    pub async fn enqueue<A>(
        &self,
        job_name: &str,
        job_data: &A,
        queue: QueueName,
        perform_at: DateTime<Utc>,
    ) -> Result<jobs::JobId>
    where
        A: prost::Message,
    {
        let job_def =
            jobs::JobDefinition::new::<A, _>(None, job_data, job_name, queue, chrono::Utc::now());
        self.enqueue_job(&job_def, perform_at).await
    }

    #[instrument(skip(self))]
    pub(crate) async fn enqueue_job(
        &self,
        job_def: &JobDefinition,
        perform_at: DateTime<Utc>,
    ) -> Result<jobs::JobId> {
        debug!("enqueuing {:?} for {}", job_def, perform_at);
        self.backend.enqueue(job_def, perform_at).await?;
        Ok(job_def.id.clone())
    }

    #[instrument(skip(self))]
    pub async fn enqueue_for_handler<T>(
        &self,
        job_data: &T::Arg,
        queue: QueueName,
        perform_at: DateTime<Utc>,
    ) -> Result<jobs::JobId>
    where
        T: JobHandler,
    {
        self.enqueue::<T::Arg>(T::NAME, job_data, queue, perform_at)
            .await
    }
}
