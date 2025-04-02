use crate::controller::redis::RedisClient::Single;
use anyhow::{anyhow, Result};
use gset::Getset;
use hex;
use r2d2::{Pool, PooledConnection};
use redis::cluster::ClusterClient;
use redis::{from_redis_value, Client, ConnectionLike, FromRedisValue, RedisError, RedisResult};
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::{OnceCell, RwLock};

#[derive(Getset, Debug, Clone)]
pub struct RedisClientOpt {
    // Network type to use, either tcp or unix.
    // Default is tcp.

    // Redis server address in "host:port" format.
    #[getset(set, vis = "pub")]
    pub addr: String,

    pub cluster_addr: Vec<String>,

    // Username to authenticate the current connection when Redis ACLs are used.
    // See: https://redis.io/commands/auth.
    // pub username: String,

    // Password to authenticate the current connection.
    // See: https://redis.io/commands/auth.
    // pub password: String,

    // Redis DB to select after connecting to a server.
    // See: https://redis.io/commands/select.
    // pub db: i8,

    // Dial timeout for establishing new connections.
    // Default is 24 hour.
    pub max_lifetime: Duration,

    // Timeout for socket reads.
    // If timeout is reached, read commands will fail with a timeout error
    // instead of blocking.
    //
    // Use value -1 for no timeout and 0 for default.
    // Default is 1 hour.
    pub idle_timeout: Duration,

    // Timeout for socket writes.
    // If timeout is reached, write commands will fail with a timeout error
    // instead of blocking.
    //
    // Use value -1 for no timeout and 0 for default.
    // Default is ReadTimout. 30s
    pub connection_timeout: Duration,

    pub connection_type: RdbType,

    // Maximum number of socket connections.
    // Default is 10 connections per every CPU as reported by runtime.NumCPU.
    pub pool_size: u32,

    pub min_pool_size: u32,
}
impl Default for RedisClientOpt {
    fn default() -> Self {
        Self {
            addr: "redis://127.0.0.1:6379/12".to_string(),
            cluster_addr: vec![],
            // username: "".to_string(),
            // password: "".to_string(),
            // db: 0,
            max_lifetime: Duration::from_secs(86400),
            idle_timeout: Duration::from_secs(3600),
            connection_timeout: Duration::from_secs(30),
            connection_type: RdbType::RdbSingle,
            pool_size: 10,
            min_pool_size: 1,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum RdbType {
    RdbSingle,
    RdbCluster,
}

pub static GLOBAL_REDIS_POOL: OnceCell<Pool<RedisConnectionManager>> = OnceCell::const_new();

pub async fn init_global_redis(conf: RedisClientOpt) -> Result<()> {
    let res = gen_redis_conn_pool(conf).await?;
    GLOBAL_REDIS_POOL.get_or_init(|| async { res }).await;
    Ok(())
}

/// 根据配置获取不同的链接
pub async fn gen_redis_conn_pool(conf: RedisClientOpt) -> Result<Pool<RedisConnectionManager>> {
    if conf.connection_type.ne(&RdbType::RdbSingle) && conf.connection_type.ne(&RdbType::RdbCluster)
    {
        return Err(anyhow::anyhow!("Redis connection type is error!"));
    }

    let manager: RedisConnectionManager;

    if conf.connection_type == RdbType::RdbSingle {
        manager = RedisConnectionManager {
            redis_client: Single(get_single(&conf).await?),
        };
    } else if conf.connection_type == RdbType::RdbCluster {
        manager = RedisConnectionManager {
            redis_client: RedisClient::Cluster(get_cluster(conf.cluster_addr).await?),
        };
    } else {
        return Err(anyhow::anyhow!("Redis connection type not supported!"));
    }
    Ok(Pool::builder()
        .max_size(conf.pool_size)
        .min_idle(Some(conf.min_pool_size))
        .connection_timeout(conf.connection_timeout)
        .max_lifetime(Some(conf.max_lifetime))
        .idle_timeout(Some(conf.idle_timeout))
        .build(manager)?)
}

/// get single redis client
async fn get_single(conf: &RedisClientOpt) -> Result<Client> {
    Ok(Client::open(&*conf.addr)?)
}

/// 获取 redis cluster 链接
async fn get_cluster(conf: Vec<String>) -> Result<ClusterClient> {
    Ok(ClusterClient::new(conf)?)
}

#[derive(Clone)]
pub enum RedisClient {
    Single(Client),
    Cluster(ClusterClient),
}

impl RedisClient {
    pub fn get_redis_connection(&self) -> RedisResult<RedisConnection> {
        match self {
            Single(s) => {
                let conn = s.get_connection()?;
                Ok(RedisConnection::Single(Box::new(conn)))
            }
            RedisClient::Cluster(c) => {
                let conn = c.get_connection()?;
                Ok(RedisConnection::Cluster(Box::new(conn)))
            }
        }
    }
}

pub enum RedisConnection {
    Single(Box<redis::Connection>),
    Cluster(Box<redis::cluster::ClusterConnection>),
}

impl RedisConnection {
    pub fn is_open(&self) -> bool {
        match self {
            RedisConnection::Single(sc) => sc.is_open(),
            RedisConnection::Cluster(cc) => cc.is_open(),
        }
    }

    pub fn query<T: FromRedisValue>(&mut self, cmd: &redis::Cmd) -> RedisResult<T> {
        match self {
            RedisConnection::Single(sc) => match sc.as_mut().req_command(cmd) {
                Ok(val) => from_redis_value(&val),
                Err(e) => Err(e),
            },
            RedisConnection::Cluster(cc) => match cc.req_command(cmd) {
                Ok(val) => from_redis_value(&val),
                Err(e) => Err(e),
            },
        }
    }
}

#[derive(Clone)]
pub struct RedisConnectionManager {
    pub redis_client: RedisClient,
}

impl r2d2::ManageConnection for RedisConnectionManager {
    type Connection = RedisConnection;
    type Error = RedisError;

    fn connect(&self) -> Result<RedisConnection, Self::Error> {
        self.redis_client.get_redis_connection()
    }

    fn is_valid(&self, conn: &mut RedisConnection) -> Result<(), Self::Error> {
        match conn {
            RedisConnection::Single(sc) => {
                let _: String = redis::cmd("PING").query(sc)?;
            }
            RedisConnection::Cluster(cc) => {
                let _: String = redis::cmd("PING").query(cc)?;
            }
        }
        Ok(())
    }

    fn has_broken(&self, conn: &mut RedisConnection) -> bool {
        !conn.is_open()
    }
}

///get_rdb
pub async fn get_rdb() -> Result<PooledConnection<RedisConnectionManager>> {
    Ok(match GLOBAL_REDIS_POOL.get() {
        None => return Err(anyhow!("rdb client is None")),
        Some(cli) => cli,
    }
    .get()?)
}

#[cfg(test)]
mod tests {
    use crate::common::func::{make_queue, make_task_key};
    use crate::common::queue::PENDING;
    use crate::common::rdb_cmd::{RDB_COMPLETED_CMD, RDB_ENQUEUE_CMD};
    use crate::controller::redis::{
        init_global_redis, make_lua_sha, RedisClientOpt, GLOBAL_REDIS_POOL,
    };
    use crate::controller::task::Task;
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_redis_client_opt() {
        let opt = RedisClientOpt::default();

        let res = init_global_redis(opt).await;
        println!("{:?}", res);
        let mut redis = GLOBAL_REDIS_POOL.get().unwrap().get().unwrap();

        let task = Task {
            task_id: Uuid::new_v4().to_string(),
            queue: "".to_string(),
            payload: "".to_string(),
            state: "".to_string(),
            retry: 0,
            retried: 0,
            error_msg: "".to_string(),
            last_failed_at: Default::default(),
            completed_at: Default::default(),
            todo_at: Default::default(),
            task_life_time: 0,
        };
        let msg = json!(task);

        let mut sql = redis::cmd("EVAL");
        sql.arg(RDB_ENQUEUE_CMD)
            .arg(2)
            .arg(make_task_key(
                task.queue.as_str(),
                "t",
                task.task_id.as_str(),
            ))
            .arg(make_queue(task.queue.as_str(), PENDING))
            .arg(msg.to_string())
            .arg(task.task_id)
            .arg(Utc::now().timestamp());

        let res: usize = redis.query(&sql).unwrap();

        print!("{}", res);
    }

    #[tokio::test]
    async fn test_lua_sha() {
        let opt = RedisClientOpt::default();

        let res = init_global_redis(opt).await;
        println!("{:?}", res);

        // let res = make_lua_sha(RDB_COMPLETED_CMD, "RDB_COMPLETED_CMD".to_string()).await;
        println!("{:?}", res);
    }
}

/////////////////////////////// lua script  /////////////////////////
pub static GLOBAL_REDIS_SCRIPT: LazyLock<Arc<RwLock<HashMap<String, String>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::with_capacity(4))));

// 初始化函数，可以在程序启动时调用

pub async fn make_lua_sha(
    conn: &mut PooledConnection<RedisConnectionManager>,
    cmd: &str,
    key: String,
) -> Result<()> {
    // let mut conn = get_rdb().await?;
    let sha = calculate_sha1(cmd);
    let _: String = conn
        .query(redis::cmd("EVAL").arg(cmd).arg(0))
        .unwrap_or_default();
    let res = {
        let read_guard = GLOBAL_REDIS_SCRIPT.read().await;
        match read_guard.get(&key) {
            None => "".to_string(),
            Some(res) => res.to_string(),
        }
    };
    if res.is_empty() {
        GLOBAL_REDIS_SCRIPT.write().await.insert(key, sha);
    }
    Ok(())
}

fn calculate_sha1(cmd: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(cmd);
    let result = hasher.finalize();
    hex::encode(result)
}
