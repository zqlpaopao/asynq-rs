use crate::common::func::{get_sha, make_queue, make_task_key};
use crate::common::queue::{COMPLETED, EVAL, EVAL_SHA, FAIL, FAIL_ED, PENDING, SUCCEEDED, T};
use crate::common::rdb_cmd::{CALLBACK, DEQUEUE, RDB_COMPLETED_CMD, RDB_DEQUEUE_CMD};
use crate::controller::redis::{get_rdb, make_lua_sha, RedisConnectionManager};
use crate::controller::task::Task;
use crate::controller::task_handler::TaskHandlerTrait;
use anyhow::{anyhow, Result};
use chrono::{Local, Utc};
use flume::{Receiver, RecvError, Sender};
use log::{debug, error};
use r2d2::PooledConnection;
use redis::ErrorKind::NoScriptError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::RwLock;
use wg::WaitGroup;

#[derive(Clone, Debug)]
pub struct QueueOption {
    //队列的消费者数量
    //也就是async future 的 数量
    pub worker: usize,

    //读取redis list 的长度
    //需要设置个合理值
    pub msg_length: usize,

    //用来缓存取出来的msg的channel的长度
    //最好是msg_length 的倍数
    //保持消费者一致在消费 不在空闲状态
    pub channel_size: usize,

    //当队列消息未空的时候的检测队列的间隔时间
    //默认是2s
    pub none_check_time: Duration,

    //获取消息的间隔时间
    //开始是500毫秒
    // 默认是1s
    pub get_msg_time: Duration,

    //处理消息的实体
    pub handler: Arc<RwLock<dyn TaskHandlerTrait<Item = (Task, String)>>>,

    // 收到结束信号 多久退出服务
    // 默认8s
    pub shutdown: Duration,

    //回推redis的次数 最好设置成redis节点数以上
    pub callback_redis_times: i32,
}

impl QueueOption {
    /// 检测参数，设置默认值
    pub async fn check(&mut self) {
        if self.worker.lt(&1) {
            self.worker = 1;
        }
        if self.get_msg_time.lt(&(Duration::from_millis(50))) {
            self.get_msg_time = Duration::from_secs(1);
        }
        if self.none_check_time.lt(&(Duration::from_secs(1))) {
            self.none_check_time = Duration::from_secs(1);
        }
        if self.msg_length.lt(&1) {
            self.msg_length = 1;
        }
        if self.shutdown.as_secs().lt(&1) {
            self.shutdown = Duration::from_secs(8);
        }
        if self.callback_redis_times.lt(&1) {
            self.callback_redis_times = 3;
        }
    }
}

#[derive(Clone, Debug)]
pub struct TaskHandler {
    queue: String,
    task_opt: QueueOption,
    done: Arc<AtomicBool>,
}

impl TaskHandler {
    pub async fn new(queue: String, task_opt: QueueOption, done: Arc<AtomicBool>) -> TaskHandler {
        Self {
            queue,
            task_opt,
            done,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut wg = WaitGroup::new();
        let (tx, rx) = flume::bounded(self.task_opt.channel_size);
        let (callback_tx, callback_rx) = flume::bounded(self.task_opt.channel_size);
        self.start_result2_redis(&mut wg, callback_rx).await;
        self.start_worker(rx, callback_tx).await;
        self.start_producer(tx).await?;
        wg.wait();
        debug!("`{}` task handler finished", self.queue);
        Ok(())
    }

    //接收处理完的任务 准备推送redis
    pub async fn start_result2_redis(&mut self, wg: &mut WaitGroup, callback_rx: Receiver<Task>) {
        let queue = self.queue.clone();
        let times = self.task_opt.callback_redis_times;
        wg.add(1);
        tokio::spawn(async move {
            loop {
                match callback_rx.recv_async().await {
                    Ok(res) => {
                        println!("callback:{:?}",res);
                        for _ in 0..times {
                            match Self::callback_redis(&res).await {
                                Ok(_) => {break}
                                Err(err) => {

                                    println!("{}",err);
                                    error!("{}",err);
                                }
                            }
                        }
                    }
                    Err(RecvError::Disconnected) => {
                        debug!(
                            "`{}`  receive  callback_redis task  error: {}",
                            &queue,
                            RecvError::Disconnected
                        );
                        println!(
                            "`{}`  receive  callback_redis task  error: {}",
                            &queue,
                            RecvError::Disconnected
                        );
                        break;
                    }
                }
            }
            debug!("`{}` start_result2_redis  is  done", &queue);
        });
        wg.done();
    }

    async fn callback_redis(task: &Task) -> Result<()> {
        let msg = serde_json::to_string(&task)?;
        let mut conn = get_rdb().await?;

        let sha = match get_sha(CALLBACK).await {
            Some(res) => res.to_string(),
            None => {
                // 如果没有找到，尝试加载脚本并重新获取 SHA1
                make_lua_sha(&mut conn, RDB_COMPLETED_CMD, CALLBACK.to_string()).await?;
                get_sha(CALLBACK)
                    .await
                    .map(|res| res.to_string())
                    .ok_or_else(|| anyhow!("taskId : {} Failed to load or retrieve SHA for CALLBACK script",task.task_id))?
            }
        };

        match Self::doing_callback_cmd(&mut conn, EVAL_SHA.to_string(), sha, task, msg.as_str())
            .await
        {
            Ok(_size) => Ok(()),
            Err(err) if err.kind() == NoScriptError => {
                error!("Script CALLBACK not found, reloading script: {}", err);
                make_lua_sha(&mut conn, RDB_COMPLETED_CMD, CALLBACK.to_string()).await?;
                Self::doing_callback_cmd(
                    &mut conn,
                    EVAL.to_string(),
                    RDB_COMPLETED_CMD.to_string(),
                    task,
                    msg.as_str(),
                )
                .await.map_err(|err|anyhow!("taskId : {} doing_callback_cmd err :{}",task.task_id,err))?;
                Ok(())
            }
            Err(err) => {
                error!("taskId : {} Client in queue failed: {}",task.task_id, err);
                Err(anyhow!(
                    "taskId : {} `{}` failed to receive task result: {}",
                    task.task_id,
                    task.queue,
                    err
                ))
            }
        }
    }

    // callback args to rdb
    async fn doing_callback_cmd(
        conn: &mut PooledConnection<RedisConnectionManager>,
        eval: String,
        cmd: String,
        task: &Task,
        msg: &str,
    ) -> Result<i8, redis::RedisError> {
        let completed_at;
        let res;
        if task.state == COMPLETED {
            res = SUCCEEDED;
            completed_at = task.completed_at.format("%Y-%m-%d %H:%M:%S").to_string();
        } else {
            res = FAIL;
            completed_at = task.last_failed_at.format("%Y-%m-%d %H:%M:%S").to_string();
        }

        let result: Result<i8, _> = conn.query(
            redis::cmd(&eval)
                .arg(&cmd)
                .arg(6)
                .arg(make_task_key(
                    task.queue.as_str(),
                    "t",
                    task.task_id.as_str(),
                ))
                .arg(make_queue(task.queue.as_str(), PENDING))
                .arg(make_queue(
                    task.queue.as_str(),
                    &(COMPLETED.to_string()
                        + &*":".to_string()
                        + &*Utc::now().format("%Y-%m-%d").to_string()),
                ))
                .arg(make_queue(
                    task.queue.as_str(),
                    &(FAIL_ED.to_string()
                        + &*":".to_string()
                        + &*Utc::now().format("%Y-%m-%d").to_string()),
                ))
                .arg(make_queue(task.queue.as_str(), COMPLETED))
                .arg(make_queue(task.queue.as_str(), FAIL_ED))
                .arg(task.task_id.as_str())
                .arg(msg)
                .arg(task.state.as_str())
                .arg(completed_at.as_str())
                .arg(task.retried)
                .arg(res)
                .arg(Duration::from_secs(90 * 24 * 3600).as_secs())
                .arg(i64::MAX),
        );

        result
    }

    pub async fn start_producer(&mut self, tx: Sender<Task>) -> Result<()> {
        let done = self.done.clone();
        let queue = self.queue.clone();
        let task_opt = self.task_opt.clone();
        tokio::spawn(async move {
            let mut local_self = TaskHandler {
                done,
                task_opt,
                queue: queue.clone(),
            };

            let mut interval = Duration::from_millis(500);
            loop {
                // println!("interval {:?}",interval);
                select! {
                   _=  tokio::time::sleep(interval) => {
                        if  local_self.done.load(Ordering::SeqCst){
                            debug!("`{}` receiver done single", queue);
                            break;
                        }

                        match local_self.dequeue(&tx).await {
                            Ok(res) => {
                                if res.lt(&1){
                                    interval = local_self.task_opt.none_check_time;
                                }else{
                                    interval = local_self.task_opt.get_msg_time;
                                }
                            }
                            Err(err) => {
                                interval = local_self.task_opt.none_check_time;
                                // println!("`{}` dequeue error: {:?}",queue,err);
                                error!("`{}` dequeue error: {:?}",queue,err);
                            }
                        }
                    }
                }
            }
            debug!("end producer tx is close {}", tx.sender_count());
            drop(tx);
        })
        .await?;
        Ok(())
    }

    async fn dequeue(&mut self, tx: &Sender<Task>) -> Result<usize> {
        let mut conn = get_rdb().await?;
        let sha = match get_sha(DEQUEUE).await {
            Some(res) => res.to_string(),
            None => {
                // 如果没有找到，尝试加载脚本并重新获取 SHA1
                make_lua_sha(&mut conn, RDB_DEQUEUE_CMD, DEQUEUE.to_string()).await?;
                get_sha(DEQUEUE)
                    .await
                    .map(|res| res.to_string())
                    .ok_or_else(|| anyhow!("Failed to load or retrieve SHA for DEQUEUE script"))?
            }
        };

        match Self::doing_dequeue_cmd(
            &mut conn,
            EVAL_SHA.to_string(),
            sha,
            self.queue.as_str(),
            self.task_opt.msg_length,
        )
        .await
        {
            Ok(arr_task) => {
                let len = arr_task.len();
                for task in arr_task.into_iter().flatten() {
                    let task_id = task.task_id.clone();
                    if let Err(e) = tx.send_async(task).await {
                        error!(
                            "`{}` queue  sending task {} to queue error : {:?}",
                            self.queue, task_id, e
                        );
                    }
                }

                Ok(len)
            }
            Err(err) if err.kind() == NoScriptError => {
                error!("Script DEQUEUE not found, reloading script: {}", err);
                make_lua_sha(&mut conn, RDB_DEQUEUE_CMD, DEQUEUE.to_string()).await?;
                Self::doing_dequeue_cmd(
                    &mut conn,
                    EVAL.to_string(),
                    RDB_DEQUEUE_CMD.to_string(),
                    self.queue.as_str(),
                    self.task_opt.msg_length,
                )
                .await?;
                Ok(0)
            }
            Err(err) => {
                error!("Client in queue failed: {}", err);
                Err(anyhow!(
                    "`{}` failed to receive task result: {}",
                    self.queue,
                    err
                ))
            }
        }
    }

    // callback args to rdb
    async fn doing_dequeue_cmd(
        conn: &mut PooledConnection<RedisConnectionManager>,
        eval: String,
        cmd: String,
        queue: &str,
        msg_length: usize,
    ) -> Result<Vec<Option<Task>>, redis::RedisError> {
        conn.query(
            redis::cmd(&eval)
                .arg(&cmd)
                .arg(2)
                .arg(make_queue(queue, PENDING))
                .arg(make_queue(
                    queue,
                    format!("{}{}{}{}", "{", T, "}", ":").as_str(),
                ))
                .arg(msg_length),
        )
    }

    // 启动任务消费程序 消费 redis取出来的任务
    async fn start_worker(&mut self, rx: Receiver<Task>, callback_tx: Sender<Task>) {
        let handler = { self.task_opt.handler.clone() };
        for x in 0..self.task_opt.worker {
            let handler = handler.clone();
            let queue = self.queue.clone();
            let rx = rx.clone();
            let callback_tx = callback_tx.clone();

            tokio::spawn(async move {
                loop {
                    match rx.recv_async().await {
                        Ok(mut task) => {
                            let mut handler_func = { handler.write().await };
                            let total_retries = task.retry + 1;
                            let task_uuid = task.task_id.clone();
                            let task_queue = task.queue.clone();

                            for _ in 0..total_retries {
                                task.todo_at = Local::now();
                                let (res, err) = handler_func.handle_task(task);
                                task = res;
                                if err.len().lt(&1) {
                                    task.completed_at = Local::now();
                                    task.state = COMPLETED.to_string();
                                    task.retried = 1;
                                    break;
                                } else {
                                    task.error_msg = err;
                                    task.retried += 1;
                                    task.last_failed_at = Local::now();
                                    task.state = FAIL_ED.to_string();
                                }
                            }

                            if let Err(err) = callback_tx.send_async(task).await {
                                error!("`{}` Worker-{}  task:{:?}  failed to send task to callback redis channel error: {}",task_queue, x, task_uuid,err);
                            }
                        }
                        Err(RecvError::Disconnected) => {
                            // Channel closed, exit the loop.
                            debug!(
                                "`{}` Worker-{}  detected channel closure. {}",
                                queue,
                                x,
                                RecvError::Disconnected
                            );
                            break;
                        }
                    }
                }
                debug!("`{}` Worker-{} is done", queue, x);
            });
        }
    }
}
