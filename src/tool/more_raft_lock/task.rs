use redis::{FromRedisValue, RedisError, RedisResult, Value};
use serde_derive::{Deserialize, Serialize};
use std::str::from_utf8;

#[derive(Deserialize, Serialize, Debug)]
pub struct LockInfo {
    pub key: String,
    pub value: u64,
    pub expire_time: String,
}
impl FromRedisValue for LockInfo {
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
                println!("{}", json_str);

                // 使用 serde_json 反序列化 JSON 字符串为 Task 结构体
                let task: LockInfo = serde_json::from_str(json_str).map_err(|e| {
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
