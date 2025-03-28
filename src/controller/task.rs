use chrono::{DateTime, Local};
use redis::{FromRedisValue, RedisError, RedisResult, Value};
use serde_derive::{Deserialize, Serialize};
use std::str::from_utf8;

#[derive(Debug, Serialize, Deserialize)]
pub struct Task {
    pub task_id: String,
    pub queue: String,
    pub payload: String,
    pub state: String,

    // Retry is the max number of retry for this task.
    pub retry: u32,

    // Retried is the number of times we've retried this task so far.
    pub retried: u32,

    // ErrorMsg holds the error message from the last failure.
    pub error_msg: String,

    // Use zero to indicate no last failure
    pub last_failed_at: DateTime<Local>,

    // CompletedAt is the time the task was processed successfully in Unix time,
    // the number of seconds elapsed since January 1, 1970, UTC.
    //
    // Use zero to indicate no value.
    // 任务完成的时间
    pub completed_at: DateTime<Local>,

    // Timeout specifies timeout in seconds.
    // If task processing doesn't complete within the timeout, the task will be retried
    // if retry count is remaining. Otherwise, it will be moved to the archive.
    //
    // Use zero to indicate no timeout.
    pub todo_at: DateTime<Local>,

    // Deadline specifies the deadline for the task in Unix time,
    // the number of seconds elapsed since January 1, 1970, UTC.
    // If task processing doesn't complete before the deadline, the task will be retried
    // if retry count is remaining. Otherwise, it will be moved to the archive.
    //
    // Use zero to indicate no deadline.
    pub task_life_time: u64,
}

impl Task {
    pub(crate) async fn check_task(&self) -> anyhow::Result<()> {
        if self.queue.is_empty() {
            return Err(anyhow::anyhow!("Task queue is empty"));
        }
        Ok(())
    }
}

impl FromRedisValue for Task {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            Value::BulkString(res) => {
                let json_str = from_utf8(res).map_err(|e| {
                    let error_message = format!("Failed to deserialize Task: {}", e);
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Response was of incompatible type",
                        format!("msg : {}", error_message),
                    ))
                })?;

                // 使用 serde_json 反序列化 JSON 字符串为 Task 结构体
                let task: Task = serde_json::from_str(json_str).map_err(|e| {
                    let error_message = format!("Failed to serde_json::from_str: {}", e);
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Response was of incompatible type",
                        format!("msg : {}", error_message),
                    ))
                })?;
                Ok(task)
            }
            _ => Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "Response was of incompatible type",
                " (response was )".to_string(),
            ))),
        }
    }
}
