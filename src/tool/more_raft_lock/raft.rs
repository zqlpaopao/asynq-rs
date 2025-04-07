use crate::common::eval::EVAL_SHA;
use crate::common::func::get_sha;
use crate::common::rdb_cmd::{
    RDB_RAFT_CMD, RDB_RAFT_DEL, RDB_RAFT_DEL_CMD, RDB_RAFT_DEL_MEMBER, RDB_RAFT_DEL_MEMBER_CMD,
    RDB_RAFT_GET_MEMBER, RDB_RAFT_GET_MEMBER_CMD, RDB_RAFT_IN, RDB_RAFT_RENEWAL,
    RDB_RAFT_RENEWAL_CMD,
};
use crate::controller::redis::{get_rdb, make_lua_sha};
use crate::tool::more_raft_lock::task::LockInfo;
use anyhow::anyhow;
use anyhow::Result;
use chrono::{Local, TimeZone, Utc};
use log::{error, warn};
use redis::ErrorKind::NoScriptError;
use redis::RedisError;
use std::ops::Add;

pub struct Raft {
    pub lock_group_name: String,
    pub lock_num: u16,
    pub current_num: u16,
    pub node_num: u16,
}

impl Raft {
    // Create a locked entity object,
    // pass in the group name of the lock lock_group_name,
    // the number of locks lock_num,
    // and the current number of rdb node_num for SHA retry
    pub fn new(lock_group_name: String, lock_num: u16, node_num: u16) -> Self {
        assert!(lock_num > 0);
        assert!(!lock_group_name.is_empty());
        Self {
            lock_group_name,
            lock_num,
            current_num: 1,
            node_num,
        }
    }

    // Retrieve the lock and set the lock time
    // Using eval sha mode, therefore
    // Suggest setting node_num to Redis node count+1
    // You can set the number of retries to be locked for trials that do not exist, and the specific locking time needs to be set by the user themselves
    // Successful locking returns 1, failure returns 0
    pub async fn lock(&self, lock_name: &str, expire: i64) -> Result<usize> {
        let mut retries = self.node_num;
        let mut conn = get_rdb().await?;
        let mut sha = make_sha(RDB_RAFT_IN, RDB_RAFT_CMD).await?;
        while retries > 0 {
            let time = Utc::now().timestamp();
            let result: Result<usize, RedisError> = conn.query(
                redis::cmd(EVAL_SHA)
                    .arg(sha)
                    .arg(2)
                    .arg(self.lock_group_name.as_str())
                    .arg(lock_name)
                    .arg(time.add(expire))
                    .arg(self.lock_num)
                    .arg(time),
            );
            return match result {
                Ok(res) => Ok(res),
                Err(err) => {
                    if is_noscript_error(&err) {
                        warn!("Script missing, reloading (retries left: {})", retries);
                        sha = make_sha(RDB_RAFT_IN, RDB_RAFT_CMD).await?;
                        retries -= 1;
                        continue;
                    }
                    error!("Redis lock error: {:#?}", err);
                    Err(anyhow!("Lock failed: {}", err))
                }
            };
        }

        Err(anyhow!("Lock max retries exceeded"))
    }

    // Renew the existing key. If the key does not exist, return 0 directly
    pub async fn renewal(&self, lock_name: &str, renewal_time: i64) -> Result<usize> {
        let mut retries = self.node_num;
        let mut conn = get_rdb().await?;
        let mut sha = make_sha(RDB_RAFT_RENEWAL, RDB_RAFT_RENEWAL_CMD).await?;
        while retries > 0 {
            let time = Utc::now().timestamp();
            let result: Result<usize, RedisError> = conn.query(
                redis::cmd(EVAL_SHA)
                    .arg(sha)
                    .arg(2)
                    .arg(self.lock_group_name.as_str())
                    .arg(lock_name)
                    .arg(time.add(renewal_time)),
            );

            let res = tidy_result(result, RDB_RAFT_RENEWAL, RDB_RAFT_RENEWAL_CMD).await;
            match res {
                Ok(res) => {
                    if res.0 == 1 || res.0 == 0 {
                        return Ok(res.0);
                    } else {
                        sha = res.1;
                        retries -= 1;
                        continue;
                    }
                }
                Err(err) => {
                    return Err(anyhow!("{} error: {:#?}", lock_name, err));
                }
            }
        }

        Err(anyhow!("Renewal max retries exceeded"))
    }

    //Deleting a key, whether it exists or not, and deleting 1 is successful
    //     let res = raft.un_lock("local2").await.unwrap();
    //     println!("un_lock local2-{:?}", res);
    pub async fn un_lock(&self, lock_name: &str) -> Result<usize> {
        let mut retries = self.node_num;
        let mut conn = get_rdb().await?;
        let mut sha = make_sha(RDB_RAFT_DEL_MEMBER, RDB_RAFT_DEL_MEMBER_CMD).await?;
        while retries > 0 {
            let result: Result<usize, RedisError> = conn.query(
                redis::cmd(EVAL_SHA)
                    .arg(sha)
                    .arg(2)
                    .arg(self.lock_group_name.as_str())
                    .arg(lock_name),
            );

            let res = tidy_result(result, RDB_RAFT_DEL_MEMBER, RDB_RAFT_DEL_MEMBER_CMD).await;
            match res {
                Ok(res) => {
                    if res.0 == 1 || res.0 == 0 {
                        return Ok(res.0);
                    } else {
                        sha = res.1;
                        retries -= 1;
                        continue;
                    }
                }
                Err(err) => {
                    return Err(anyhow!("{} error: {:#?}", lock_name, err));
                }
            }
        }

        Err(anyhow!("Unlock max retries exceeded"))
    }

    // Delete the entire group
    // Enter groupname
    pub async fn un_lock_all(&self, group_name: &str) -> Result<usize> {
        let mut retries = self.node_num;
        let mut conn = get_rdb().await?;
        let mut sha = make_sha(RDB_RAFT_DEL, RDB_RAFT_DEL_CMD).await?;
        while retries > 0 {
            let result: Result<usize, RedisError> = conn.query(
                redis::cmd(EVAL_SHA)
                    .arg(sha)
                    .arg(1)
                    .arg(self.lock_group_name.as_str()),
            );

            let res = tidy_result(result, RDB_RAFT_DEL, RDB_RAFT_DEL_CMD).await;
            match res {
                Ok(res) => {
                    if res.0 == 1 || res.0 == 0 {
                        return Ok(res.0);
                    } else {
                        sha = res.1;
                        retries -= 1;
                        continue;
                    }
                }
                Err(err) => {
                    return Err(anyhow!("{} error: {:#?}", group_name, err));
                }
            }
        }

        Err(anyhow!("UnlockAll max retries exceeded"))
    }

    // The situation of obtaining all locks
    pub async fn get_members(&self) -> Result<Vec<LockInfo>> {
        let mut retries = self.node_num;
        let mut conn = get_rdb().await?;
        let mut sha = make_sha(RDB_RAFT_GET_MEMBER, RDB_RAFT_GET_MEMBER_CMD).await?;
        while retries > 0 {
            let result: Result<Vec<(String, u64)>, RedisError> = conn.query(
                redis::cmd(EVAL_SHA)
                    .arg(sha)
                    .arg(1)
                    .arg(self.lock_group_name.as_str()),
            );

            return match result {
                Ok(res) => Ok(res
                    .into_iter()
                    .map(|pairs| LockInfo {
                        key: pairs.0,
                        value: pairs.1,
                        expire_time: timestamp_to_datetime(pairs.1 as i64).unwrap_or_default(),
                    })
                    .collect()),
                Err(err) => {
                    if is_noscript_error(&err) {
                        warn!("Script missing, reloading (retries left: {})", retries);
                        sha = make_sha(RDB_RAFT_GET_MEMBER, RDB_RAFT_GET_MEMBER_CMD).await?;
                        retries -= 1;
                        continue;
                    }
                    error!("Redis lock error: {:#?}", err);
                    Err(anyhow!("Lock failed: {}", err))
                }
            };
        }

        Err(anyhow!("GetMember max retries exceeded"))
    }
}

// Determine whether there is no NoScriptError or
// if the returned TypeError contains NoScriptError
fn is_noscript_error(err: &RedisError) -> bool {
    err.kind() == NoScriptError || err.to_string().contains("NOSCRIPT")
}

// get redis sha
async fn make_sha(key: &str, cmd: &str) -> Result<String> {
    let mut conn = get_rdb().await?;
    match get_sha(key).await {
        Some(res) => Ok(res.to_string()),
        None => {
            make_lua_sha(&mut conn, cmd, key.to_string()).await?;
            Ok(get_sha(key)
                .await
                .map(|res| res.to_string())
                .ok_or_else(|| anyhow!("Failed to load SHA"))?)
        }
    }
}

// Determine whether there is no NoScriptError or
// if the returned TypeError contains NoScriptError
async fn tidy_result(
    result: Result<usize, RedisError>,
    key: &str,
    cmd: &str,
) -> Result<(usize, String)> {
    match result {
        Ok(res) => Ok((res, "".to_string())),
        Err(err) => {
            if is_noscript_error(&err) {
                println!("{} :Script missing, reloading err : {} ", key, err);
                warn!("{} :Script missing, reloading err : {} ", key, err);
                return Ok((2_usize, make_sha(key, cmd).await?));
            }
            error!("{} : Redis lock error: {:#?}", key, err);
            Err(anyhow!("tidyResult lock failed: {}", err))
        }
    }
}

fn timestamp_to_datetime(seconds: i64) -> Result<String> {
    Local
        .timestamp_opt(seconds, 0)
        .single()
        .ok_or_else(|| anyhow!("Invalid timestamp"))
        .map(|datetime| datetime.format("%Y-%m-%d %H:%M:%S").to_string())
}
