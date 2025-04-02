use crate::controller::handler::{QueueOption, TaskHandler};
use anyhow::Result;
use log::{debug, error};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use wg::WaitGroup;

pub struct Server {
    done: Arc<AtomicBool>,
    wg: WaitGroup,
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    /// 初始化server
    pub fn new() -> Self {
        Server {
            done: Arc::new(AtomicBool::new(false)),
            wg: WaitGroup::new(),
        }
    }

    /// 开始 server 服务
    pub async fn start(&mut self, queue_map: HashMap<String, QueueOption>) -> Result<()> {
        if queue_map.is_empty() {
            return Ok(());
        }
        // self.init_lua_sha().await?;
        self.wg.add(queue_map.len());

        for (queue_name, mut queue_option) in queue_map {
            queue_option.check().await;
            let done = self.done.clone();
            let wg = self.wg.clone();
            tokio::spawn(async move {
                match TaskHandler::new(queue_name.clone(), queue_option, done)
                    .await
                    .run()
                    .await
                {
                    Ok(res) => {
                        debug!("`{}` is start running success {:?}", queue_name, res);
                    }
                    Err(err) => {
                        error!("`{}` failed to start: {}", queue_name, err);
                    }
                }
                wg.done();
            });
        }
        Ok(())
    }

    pub async fn stop(&mut self) {
        self.done.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub async fn wait(&mut self) {
        self.wg.wait();
    }
}

#[cfg(test)]
mod tests {
    use crate::controller::handler::QueueOption;
    use crate::controller::redis::{init_global_redis, RedisClientOpt};
    use crate::controller::server::Server;
    use crate::controller::task::Task;
    use crate::controller::task_handler::TaskHandlerTrait;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_server() {
        #[derive(Debug)]
        pub struct TestHandler {}
        impl TaskHandlerTrait for TestHandler {
            type Item = (Task, String);

            fn handle_task(&mut self, mut task: Task) -> Self::Item {
                println!("handler {:?}", task);
                let payload: i32 = task.payload.parse().unwrap();
                if payload % 2 == 0 {
                    (task, "test error".to_string())
                } else {
                    (task, "".to_string())
                }
            }
        }

        //初始化redis client
        let _ = init_global_redis(RedisClientOpt::default()).await.unwrap();

        let mut server = Server::new();

        let res = server
            .start(HashMap::from([
                (
                    "test".to_string(),
                    QueueOption {
                        worker: 200,
                        msg_length: 1,
                        channel_size: 50,
                        none_check_time: Duration::from_secs(1),
                        get_msg_time: Duration::from_millis(100),
                        handler: Arc::new(RwLock::new(TestHandler {})),
                        shutdown: Default::default(),
                        callback_redis_times: 0,
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
        println!("{:?}", res);
        println!("server start");

        // runtime.block_on(async {
        //     println!("server start");
        //
        // });
        sleep(Duration::from_secs(10)).await;

        // server.stop().await;

        sleep(Duration::from_secs(500)).await;

        // server.stop().await.unwrap();
        // println!("server stopped");
    }
}
