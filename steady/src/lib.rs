mod backends;
mod consumer;
mod cron;
mod error;
mod jobs;
mod producer;

pub use crate::cron::CronScheduler;
pub use async_trait::async_trait;
pub use backends::memory::Backend as MemoryBackend;
#[cfg(feature = "backend-redis")]
pub use backends::redis::Backend as RedisBackend;
pub use consumer::Consumer;
pub use error::{Error, ErrorHandler, Result};
pub use jobs::{JobHandler, JobId, QueueName};
pub use producer::Producer;

pub type DateTime<T> = chrono::DateTime<T>;
pub type Utc = chrono::Utc;
pub type Duration = chrono::Duration;
