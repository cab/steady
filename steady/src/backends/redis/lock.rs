use std::{
    convert::{TryFrom, TryInto},
    num::TryFromIntError,
    time::{Duration, SystemTime},
};

use lazy_static::lazy_static;
use nanoid::nanoid;
use rand::{thread_rng, Rng};
use redis::ToRedisArgs;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("at least one redis client is required")]
    NoRedisClients,
    #[error("todo")]
    Todo,
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
    #[error("unable to lock")]
    UnableToLock(#[source] Box<dyn std::error::Error>),
    #[error("unable to unlock")]
    UnableToUnlock,
    #[error("unable to extend")]
    UnableToExtend(#[source] Box<dyn std::error::Error>),
    #[error("invalid ttl: {0}")]
    InvalidTtl(#[source] std::num::TryFromIntError),
    #[error("lock expired {0:?} ago")]
    LockExpired(Option<std::time::Duration>),
    #[error("already locked")]
    AlreadyLocked,
}

type Result<T> = std::result::Result<T, Error>;

lazy_static! {
    static ref SCRIPT_LOCK: redis::Script =
        redis::Script::new(r#"return redis.call("set", KEYS[1], ARGV[1], "NX", "PX", ARGV[2])"#);
    static ref SCRIPT_UNLOCK: redis::Script = redis::Script::new(
        r#"
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
            "#
    );
    static ref SCRIPT_EXTEND: redis::Script = redis::Script::new(
        r#"
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("pexpire", KEYS[1], ARGV[2])
            else
                return 0
            end
            "#
    );
}

#[derive(Debug, Clone)]
enum Request {
    Lock,
    Extend { resource_value: String },
}

#[derive(Debug)]
pub struct Redlock {
    clients: Vec<redis::Client>,
    retry_count: u32,
    retry_delay: Duration,
    retry_jitter: u32,
    drift_factor: f32,
    quorum: usize,
}

impl Redlock {
    fn new(clients: impl IntoIterator<Item = redis::Client>) -> Result<Self> {
        let clients = clients.into_iter().collect::<Vec<_>>();
        if clients.is_empty() {
            return Err(Error::NoRedisClients);
        }
        let quorum = (clients.len() as f64 / 2_f64).floor() as usize + 1;

        // TODO allow configuration
        let drift_factor = 0.01f32;
        let retry_jitter = 400;
        let retry_delay = Duration::from_millis(400);
        let retry_count = 10;

        Ok(Self {
            clients,
            drift_factor,
            retry_count,
            retry_delay,
            retry_jitter,
            quorum,
        })
    }

    pub async fn lock<'a>(&'a self, resource_name: &str, ttl: Duration) -> Result<Lock<'a>> {
        self.request(Request::Lock, resource_name, ttl).await
    }

    async fn request<'a>(
        &'a self,
        request: Request,
        resource_key: &str,
        ttl: Duration,
    ) -> Result<Lock<'a>> {
        let mut attempts = 0;
        let drift = Duration::from_millis(
            (self.drift_factor as f64 * ttl.as_millis() as f64).round() as u64 + 2,
        );

        let mut last_error: Option<Box<dyn std::error::Error>> = None;

        'retry: while attempts < self.retry_count {
            attempts += 1;

            let mut waitings = self.clients.len();
            let mut votes = 0;
            let mut errors = 0;

            let start = SystemTime::now();

            let value = match &request {
                Request::Lock => nanoid!(),
                Request::Extend { resource_value } => resource_value.clone(),
            };

            for client in &self.clients {
                let locked = Lock {
                    redlock: &self,
                    resource_key: resource_key.into(),
                    value: value.clone(),
                    expiration: start + ttl - drift,
                };

                let result = match request {
                    Request::Lock => lock(client, resource_key, &value, &ttl).await,
                    Request::Extend { .. } => extend(client, resource_key, &value, &ttl).await,
                };

                match result {
                    Ok(success) => {
                        waitings -= 1;

                        if !success {
                            continue;
                        }

                        votes += 1;
                        if waitings > 0 {
                            continue;
                        }

                        if votes >= self.quorum && locked.expiration > SystemTime::now() {
                            return Ok(locked);
                        }

                        locked.unlock().await.unwrap(); // TODO handle error
                        tokio::time::sleep(self.retry_timeout()).await;
                        continue 'retry;
                    }
                    Err(err) => {
                        last_error = Some(Box::new(err));
                        errors += 1;

                        if errors > self.quorum {
                            locked.unlock().await.unwrap(); // TODO handle error
                            tokio::time::sleep(self.retry_timeout()).await;
                            continue 'retry;
                        }
                    }
                }
            }
        }

        Err(match request {
            // TODO verify this unwrap is safe
            Request::Lock => {
                Error::UnableToLock(last_error.unwrap_or_else(|| Box::new(Error::AlreadyLocked)))
            }
            Request::Extend { .. } => {
                Error::UnableToExtend(last_error.unwrap_or_else(|| Box::new(Error::AlreadyLocked)))
            }
        })
    }

    async fn extend<'a>(
        &'a self,
        resource_name: &str,
        value: impl Into<String>,
        ttl: Duration,
    ) -> Result<Lock<'a>> {
        self.request(
            Request::Extend {
                resource_value: value.into(),
            },
            resource_name,
            ttl,
        )
        .await
    }

    async fn unlock(&self, resource_name: &str, value: &str) -> Result<()> {
        let mut attempts = 0;

        'attempts: while attempts < self.retry_count {
            attempts += 1;

            let mut waitings = self.clients.len();
            let mut votes = 0;
            let mut errors = 0;

            for client in &self.clients {
                match unlock(client, resource_name, value).await {
                    Ok(success) => {
                        waitings -= 1;
                        if !success {
                            continue;
                        }

                        votes += 1;
                        if waitings > 0 {
                            continue;
                        }
                        if votes >= self.quorum {
                            return Ok(());
                        }
                    }
                    Err(_) => {
                        errors += 1;
                        // This attempt is doomed to fail, will retry after
                        // the timeout
                        if errors >= self.quorum {
                            tokio::time::sleep(self.retry_timeout()).await;
                            continue 'attempts;
                        }
                    }
                }
            }
        }

        Err(Error::UnableToUnlock)
    }

    fn retry_timeout(&self) -> Duration {
        let jitter = self.retry_jitter as i32 * thread_rng().gen_range(-1..=1);
        if jitter >= 0 {
            self.retry_delay + Duration::from_millis(jitter as u64)
        } else {
            self.retry_delay - Duration::from_millis(-jitter as u64)
        }
    }
}

#[derive(Debug)]
pub struct Lock<'a> {
    redlock: &'a Redlock,
    resource_key: String,
    value: String,
    expiration: SystemTime,
}

impl<'a> Lock<'a> {
    async fn unlock(self) -> std::result::Result<(), (Lock<'a>, Error)> {
        self.redlock
            .unlock(&self.resource_key, &self.value)
            .await
            .map_err(|err| (self, err))
    }

    async fn extend(&mut self, ttl: Duration) -> Result<()> {
        let now = SystemTime::now();
        if self.expiration < now {
            return Err(Error::LockExpired(now.duration_since(self.expiration).ok()));
        }

        let mut new_lock = self
            .redlock
            .extend(&self.resource_key, &self.value, ttl)
            .await?;

        std::mem::swap(self, &mut new_lock);
        Ok(())
    }
}

async fn lock(
    client: &redis::Client,
    resource_name: &str,
    value: &str,
    ttl: &Duration,
) -> Result<bool> {
    match SCRIPT_LOCK
        .key(String::from(resource_name))
        .arg(String::from(value))
        .arg(ttl.as_millis() as u64)
        .invoke_async::<_, Option<()>>(&mut client.get_async_connection().await?)
        .await?
    {
        Some(_) => Ok(true),
        _ => Ok(false),
    }
}

async fn unlock(client: &redis::Client, resource_name: &str, value: &str) -> Result<bool> {
    match SCRIPT_UNLOCK
        .key(resource_name)
        .arg(value)
        .invoke_async::<_, i32>(&mut client.get_async_connection().await?)
        .await?
    {
        1 => Ok(true),
        _ => Ok(false),
    }
}

async fn extend(
    client: &redis::Client,
    resource_name: &str,
    value: &str,
    ttl: &Duration,
) -> Result<bool> {
    match SCRIPT_EXTEND
        .key(resource_name)
        .arg(value)
        .arg(u64::try_from(ttl.as_millis()).map_err(Error::InvalidTtl)?)
        .invoke_async::<_, i32>(&mut client.get_async_connection().await?)
        .await?
    {
        1 => Ok(true),
        _ => Ok(false),
    }
}

pub async fn with_lock<F, R>(
    clients: impl IntoIterator<Item = redis::Client>,
    resource_key: &str,
    ttl: Duration,
    f: F,
) -> Result<R>
where
    F: Fn() -> R,
{
    let redlock = Redlock::new(clients)?;
    let lock = redlock.lock(resource_key, ttl).await?;
    let result = f();
    lock.unlock().await.map_err(|(_, err)| err)?;
    Ok(result)
}

#[cfg(test)]
mod tests {

    use super::*;
    use redis::Commands;

    lazy_static! {
        static ref CLIENTS: Vec<redis::Client> =
            vec![redis::Client::open("redis://127.0.0.1").unwrap()];
        static ref REDLOCK: Redlock = Redlock::new(CLIENTS.clone()).unwrap();
        static ref REDIS_CLI: redis::Client = redis::Client::open("redis://127.0.0.1").unwrap();
    }

    #[tokio::test]
    async fn test_lock() {
        let resource_name = "test_lock";
        let one_second = Duration::from_millis(1000);

        let lock = REDLOCK.lock(resource_name, one_second).await.unwrap();
        assert!(lock.expiration < SystemTime::now() + one_second);
    }

    #[tokio::test]
    async fn test_with_lock() {
        let resource_key = "test_with_lock";
        let one_second = Duration::from_millis(1000);
        let out = with_lock(CLIENTS.clone(), resource_key, one_second, || 1)
            .await
            .unwrap();
        assert_eq!(out, 1);
    }

    #[tokio::test]
    async fn test_lock_twice() {
        let resource_name = "test_lock_twice";
        let one_second = Duration::from_millis(1000);
        let start = SystemTime::now();
        let lock = REDLOCK.lock(resource_name, one_second).await.unwrap();

        assert!(lock.expiration > start);
        assert!(lock.expiration < start + (one_second));
        assert!(REDLOCK.lock(resource_name, one_second).await.is_err());

        tokio::time::sleep(one_second).await;

        assert!(REDLOCK.lock(resource_name, one_second).await.is_ok());
    }

    #[tokio::test]
    async fn test_unlock() {
        let resource_name = "test_unlock";
        let lock = REDLOCK
            .lock(resource_name, Duration::from_millis(2000))
            .await
            .unwrap();

        let value: String = REDIS_CLI
            .get_connection()
            .unwrap()
            .get(resource_name)
            .unwrap();
        assert_eq!(value.len(), 21); // nanoid is 21 chars

        lock.unlock().await.unwrap();
        let res: Option<String> = REDIS_CLI
            .get_connection()
            .unwrap()
            .get(resource_name)
            .unwrap();
        assert!(res.is_none());
    }

    #[tokio::test]
    async fn test_extend() {
        let resource_name = "test_extend";
        let mut lock = REDLOCK
            .lock(resource_name, Duration::from_millis(2000))
            .await
            .unwrap();
        lock.extend(Duration::from_millis(2000)).await.unwrap();

        assert!(lock.expiration < SystemTime::now() + (Duration::from_millis(2000)));
    }

    #[tokio::test]
    async fn test_extend_expired_resource() {
        let one_second = Duration::from_millis(1000);
        let resource_name = "test_extend_expired_resource";
        let mut lock = REDLOCK.lock(resource_name, one_second).await.unwrap();
        tokio::time::sleep(one_second * 2).await;
        assert!(lock.extend(one_second).await.is_err());
    }
}
