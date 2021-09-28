use crate::{
    backends,
    error::Result,
    jobs::{self, JobDefinition, QueueName},
    JobHandler,
};
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
    ) -> Result<jobs::JobId>
    where
        A: prost::Message,
    {
        let job_def =
            jobs::JobDefinition::new::<A, _>(job_data, job_name, queue, chrono::Utc::now())?;
        self.enqueue_job(job_def).await
    }

    #[instrument(skip(self))]
    pub(crate) async fn enqueue_job(&self, job_def: JobDefinition) -> Result<jobs::JobId> {
        debug!("enqueuing {:?}", job_def);
        self.backend.enqueue(&job_def).await?;
        Ok(job_def.id)
    }

    #[instrument(skip(self))]
    pub async fn enqueue_for_handler<T>(
        &self,
        job_data: &T::Arg,
        queue: QueueName,
    ) -> Result<jobs::JobId>
    where
        T: JobHandler,
    {
        self.enqueue::<T::Arg>(T::NAME, job_data, queue).await
    }
}
