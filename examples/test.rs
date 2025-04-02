// examples/test.rs

use asynq_rs::controller::client::Client;
use asynq_rs::controller::handler::QueueOption;
use asynq_rs::controller::redis::{init_global_redis, RdbType, RedisClientOpt};
use asynq_rs::controller::server::Server;
use asynq_rs::controller::task::Task;
use asynq_rs::controller::task_handler::TaskHandlerTrait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use uuid::Uuid;
use fast_log::consts::LogSize;
use fast_log::plugin::file_split::{KeepType, RawFile, Rolling, RollingType};
use fast_log::plugin::packer::LogPacker;
use fast_log::{ Config};
use log::LevelFilter;
use tokio::spawn;

#[tokio::main]
async fn main() {
    init_log();
    let opt = RedisClientOpt {
        addr: "redis://127.0.0.1:6379/12".to_string(),
        // addr: "redis://127.0.0.1:6379/12".to_string(),
        cluster_addr: vec![
            "redis://127.0.0.1:6380".to_string(),
            "redis://127.0.0.1:6381".to_string(),
            "redis://127.0.0.1:6382".to_string(),
        ],
        max_lifetime: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(5),
        connection_timeout: Duration::from_secs(5),
        connection_type: RdbType::RdbCluster,
        // connection_type: RdbType::RdbSingle,
        pool_size: 10,
        min_pool_size: 3,
    };

    let _ = init_global_redis(opt).await.unwrap();
    // let _ = init_global_redis(opt).await.unwrap();

    spawn(async move {
        test_client().await;

    });

    spawn(async move {
        test_server().await;

    });
    tokio::time::sleep(Duration::from_secs(1000000000)).await;


}

async fn test_client() {
    let mut client = Client::new().await;

    for i in 1..=1000000 {
        // tokio::time::sleep(Duration::from_millis(200)).await;
        println!("Sending message {}", i);
        client
            .in_queue(Task {
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
                task_life_time: 18000,
            })
            .await
            .unwrap();
    }
}

#[derive(Debug)]
pub struct TestHandler {}
impl TaskHandlerTrait for TestHandler {
    type Item = (Task, String);

    fn handle_task(&mut self, task: Task) -> Self::Item {
        println!("handler {:?}", task);
        let payload: i32 = task.payload.parse().unwrap();
        if payload % 2 == 0 {
            (task, "test error".to_string())
        } else {
            (task, "".to_string())
        }
    }
}
async fn test_server() {
    let mut server = Server::new();
    let res = server
        .start(HashMap::from([
            (
                "test".to_string(),
                QueueOption {
                    worker: 50000,
                    msg_length: 5000,
                    channel_size: 50000,
                    none_check_time: Duration::from_secs(1),
                    get_msg_time: Duration::from_millis(100),
                    handler: Arc::new(RwLock::new(TestHandler {})),
                    shutdown: Default::default(),
                    callback_redis_times: 3,
                },
            ),
            (
                "test1-----".to_string(),
                QueueOption {
                    worker: 20,
                    msg_length: 1,
                    channel_size: 5,
                    none_check_time: Duration::from_secs(1),
                    get_msg_time: Duration::from_millis(100),
                    handler: Arc::new(RwLock::new(TestHandler {})),
                    shutdown: Default::default(),
                    callback_redis_times: 0,
                },
            ),
        ]))
        .await;
    println!("server start {:?}", res);

    // sleep(Duration::from_secs(10)).await;

    // server.stop().await;
    println!("wait start");

    // server.wait().await;

    // sleep(Duration::from_secs(5)).await;
}



pub(crate) fn init_log() {
    fast_log::init(
        Config::new()
            .chan_len(Some(100000))
            .split::<RawFile, _, _, _>(
                "target/ logs/ temp. log",
                KeepType::All,
                LogPacker {},
                Rolling::new(RollingType::BySize(LogSize::MB(1))),
            )
            .console()
            .level(LevelFilter::Debug),
    )
    .unwrap();
}
