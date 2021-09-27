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
    pub async fn schedule<R>(&self, job_data: &R::Arg, queue: QueueName) -> Result<jobs::JobId>
    where
        R: JobHandler,
    {
        let job_def = jobs::JobDefinition::new::<R::Arg>(
            job_data,
            R::NAME.to_string(),
            queue,
            chrono::Utc::now(),
        )?;
        debug!("scheduling {:?}", job_def);
        self.backend.schedule(&job_def).await?;
        Ok(job_def.id)
    }
}
