use std::marker::PhantomData;

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    backends,
    scheduler::{self, ManagerAction},
    Scheduler,
};

pub struct CronScheduler<Backend> {
    manager_tx: UnboundedSender<ManagerAction>,
    pt: PhantomData<Backend>,
}

impl<Backend> CronScheduler<Backend>
where
    Backend: backends::Backend + 'static,
{
    pub fn for_scheduler(scheduler: &Scheduler<Backend>) -> Self {
        Self::new(scheduler.action_tx())
    }

    fn new(manager_tx: UnboundedSender<ManagerAction>) -> Self {
        Self {
            manager_tx,
            pt: PhantomData,
        }
    }
}
