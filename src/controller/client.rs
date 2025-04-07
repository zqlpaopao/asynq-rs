use crate::common::eval::{EVAL, EVAL_SHA};
use crate::common::func::{get_sha, make_queue, make_task_key};
use crate::common::queue::PENDING;
use crate::common::rdb_cmd::{ENQUEUE, RDB_ENQUEUE_CMD};
use crate::controller::redis::{get_rdb, make_lua_sha, RedisConnectionManager};
use crate::controller::task::Task;
use anyhow::{anyhow, Result};
use chrono::Utc;
use log::error;
use r2d2::PooledConnection;
use redis::ErrorKind;
use serde_json::json;

#[derive(Debug)]
pub struct Client {}

impl Client {
    pub async fn new() -> Self {
        Client {}
    }
    // 获取script sha

    // KEYS[1] -> ZQL:{<qname>}:t:<task_id>
    // KEYS[2] -> ZQL:{<qname>}:pending
    // --
    // ARGV[1] -> task message data
    // ARGV[2] -> task ID
    // ARGV[3] -> current unix time in nsec
    pub async fn in_queue(&mut self, task: Task) -> Result<()> {
        task.check_task().await?;
        let mut conn = get_rdb().await?;

        // 尝试从全局缓存中获取 SHA1
        let sha = match get_sha(ENQUEUE).await {
            Some(res) => res.to_string(),
            None => {
                // 如果没有找到，尝试加载脚本并重新获取 SHA1
                make_lua_sha(&mut conn, RDB_ENQUEUE_CMD, ENQUEUE.to_string()).await?;
                get_sha(ENQUEUE)
                    .await
                    .map(|res| res.to_string())
                    .ok_or_else(|| anyhow!("Failed to load or retrieve SHA for ENQUEUE script"))?
            }
        };

        match self
            .doing(&mut conn, EVAL_SHA.to_string(), sha, &task)
            .await
        {
            Ok(_size) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NoScriptError => {
                error!("Script ENQUEUE not found, reloading script: {}", err);
                make_lua_sha(&mut conn, RDB_ENQUEUE_CMD, ENQUEUE.to_string()).await?;
                self.doing(
                    &mut conn,
                    EVAL.to_string(),
                    RDB_ENQUEUE_CMD.to_string(),
                    &task,
                )
                .await?;
                Ok(())
            }
            Err(err) => {
                error!("Client in queue failed: {}", err);
                Err(err.into())
            }
        }
    }
    async fn doing(
        &self,
        conn: &mut PooledConnection<RedisConnectionManager>,
        eval: String,
        cmd: String,
        task: &Task,
    ) -> Result<usize, redis::RedisError> {
        let result: Result<usize, _> = conn.query(
            redis::cmd(&eval)
                .arg(&cmd)
                .arg(2)
                .arg(make_task_key(
                    task.queue.as_str(),
                    "t",
                    task.task_id.as_str(),
                ))
                .arg(make_queue(task.queue.as_str(), PENDING))
                .arg(json!(task).to_string())
                .arg(&task.task_id)
                .arg(Utc::now().timestamp())
                .arg(task.task_life_time),
        );

        result
    }
}

#[cfg(test)]
mod tests {
    use crate::controller::client::Client;
    use crate::controller::redis::{init_global_redis, RedisClientOpt};
    use crate::controller::task::Task;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_client_in_queue() {
        //初始化redis client
        let _ = init_global_redis(RedisClientOpt::default()).await.unwrap();

        let mut client = Client::new().await;
        for i in 0..10 {
            // time::sleep(time::Duration::from_secs(1)).await;
            client
                .in_queue(Task {
                    // task_id: Uuid::new_v4().to_string() + &i.to_string(),
                    task_id: Uuid::new_v4().to_string(),
                    queue: "test".to_string(),
                    payload: i.to_string(),
                    state: "pending".to_string(),
                    retry: 3,
                    retried: 0,
                    error_msg: "".to_string(),
                    last_failed_at: Default::default(),
                    completed_at: Default::default(),
                    todo_at: Default::default(),
                    task_life_time: 180,
                })
                .await
                .unwrap()
        }
    }
}
