use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use tracing::error;

use crate::jobs::{JobDefinition, JobId};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
    #[error(transparent)]
    Serialization(#[from] bincode::Error),
    #[error("handler already registered for name '{0}'")]
    HandlerAlreadyRegistered(&'static str),
    #[error("handler not found for job {0:?}")]
    NoHandler(JobDefinition),
    #[error("job {} ({}) failed: {}", .0.job_name, .0.id, .1)]
    JobFailed(JobDefinition, #[source] StdError),
    #[error("job failures: {0:?}")]
    JobFailures(Vec<Error>),
    #[error(transparent)]
    Cron(#[from] cron::error::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub trait ErrorHandler: Send + Sync {
    fn job_failed(&self, job_id: &JobId, job_name: &str, error: &Error);
}

#[derive(Clone, Default)]
pub struct ErrorHandlers {
    handlers: Arc<Mutex<Vec<Box<dyn ErrorHandler>>>>,
}

impl std::fmt::Debug for ErrorHandlers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorHandlers").finish()
    }
}

impl ErrorHandlers {
    pub(crate) fn add(&mut self, handler: impl ErrorHandler + 'static) {
        self.handlers.lock().unwrap().push(Box::new(handler));
    }
}

impl ErrorHandler for ErrorHandlers {
    fn job_failed(&self, job_id: &JobId, job_name: &str, error: &Error) {
        match self.handlers.lock() {
            Ok(handlers) => {
                for handler in handlers.iter() {
                    handler.job_failed(job_id, job_name, error);
                }
            }
            Err(e) => {
                error!("failed to lock handlers: {}", e);
            }
        }
    }
}
