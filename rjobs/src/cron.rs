use std::marker::PhantomData;

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    backends,
    producer::{Producer, ProducerAction},
};

pub struct CronScheduler<Backend> {
    producer_tx: UnboundedSender<ProducerAction>,
    pt: PhantomData<Backend>,
}

impl<Backend> CronScheduler<Backend>
where
    Backend: backends::Backend + 'static,
{
    pub fn for_scheduler(producer: &Producer<Backend>) -> Self {
        Self::new(producer.action_tx())
    }

    fn new(producer_tx: UnboundedSender<ProducerAction>) -> Self {
        Self {
            producer_tx,
            pt: PhantomData,
        }
    }
}
