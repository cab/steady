use crate::{jobs::JobDefinition, scheduler::QueueName, Result};
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Default)]
pub struct Backend {
    jobs_by_queue: Arc<Mutex<RefCell<HashMap<QueueName, VecDeque<JobDefinition>>>>>,
}

#[async_trait::async_trait]
impl super::Backend for Backend {
    async fn pull(&self, queue: &QueueName, count: NonZeroUsize) -> Result<Vec<JobDefinition>> {
        if let Some(values) = self
            .jobs_by_queue
            .lock()
            .unwrap() // todo
            .borrow_mut()
            .get_mut(queue)
        {
            let max = std::cmp::min(values.len(), count.get());
            let jobs = values.drain(0..max);
            Ok(jobs.into_iter().collect())
        } else {
            Ok(vec![])
        }
    }

    async fn schedule(&self, queue: &QueueName, job_def: &JobDefinition) -> Result<()> {
        self.jobs_by_queue
            .lock()
            .unwrap() // todo
            .borrow_mut()
            .entry(queue.clone())
            .or_insert_with(VecDeque::new)
            .push_back(job_def.clone());
        Ok(())
    }
}
