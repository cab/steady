use crate::{
    backends,
    error::Result,
    jobs::{self, QueueName},
    JobHandler,
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{debug, instrument};

#[derive(Debug)]
pub enum ProducerAction {}

pub struct Producer<Backend> {
    backend: Backend,
    action_tx: UnboundedSender<ProducerAction>,
    action_rx: UnboundedReceiver<ProducerAction>,
}

impl<Backend> Producer<Backend> {
    pub(crate) fn action_tx(&self) -> UnboundedSender<ProducerAction> {
        self.action_tx.clone()
    }
}

impl<Backend> Producer<Backend>
where
    Backend: backends::Backend + 'static,
{
    pub fn new(backend: Backend) -> Self {
        let (action_tx, action_rx) = unbounded_channel();
        Self {
            backend,
            action_rx,
            action_tx,
        }
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
        debug!("scheduling {:?}", job_def);
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
