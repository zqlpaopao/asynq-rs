use asynq_rs::controller::redis::{init_global_redis, RdbType, RedisClientOpt};
use asynq_rs::tool::more_raft_lock::raft::Raft;
use fast_log::consts::LogSize;
use fast_log::plugin::file_split::{KeepType, RawFile, Rolling, RollingType};
use fast_log::plugin::packer::LogPacker;
use fast_log::Config;
use log::LevelFilter;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use chrono::Local;
use tokio::select;
use tokio::sync::{broadcast, RwLock};
use get_local_info::get_pc_ipv4;

// cargo run  --example  raft --features raft_lock
#[tokio::main]
async fn main() {
    init_log();
    let group_name = "test";
    let lock_num = 1;
    let node_num = 3;

    let mut handles = vec![];

    for i in 0..=10 {
        let handle = tokio::spawn(async move {
            make_raft_lock(
                group_name,
                &format!("{}_{}", get_pc_ipv4(),i),
                lock_num,
                node_num,
            ).await;
        });
        handles.push(handle);
    }

    // 等待所有任务完成
    for handle in handles {
        handle.await.unwrap();
    }

    tokio::time::sleep(Duration::from_secs(100000)).await;
}

async fn make_raft_lock(group_name: &str, lock_namec: &str, lock_num: usize, node_num: usize) {
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
        // connection_type: RdbType::RdbCluster,
        connection_type: RdbType::RdbSingle,
        pool_size: 10,
        min_pool_size: 3,
    };

    let _ = init_global_redis(opt).await.unwrap();
    let  raft = Arc::new(Raft::new(
        group_name.to_string(),
        lock_num as u16,
        node_num as u16,
    ));

    raft.un_lock(lock_namec).await.unwrap();

    let break_flag = Arc::new(RwLock::new(AtomicBool::new(false)));
    loop {
        let lock;
        {
            lock = raft.lock(lock_namec, 60).await.unwrap_or_default()
        }
        if  lock == 1 {
            let raft_clone = raft.clone();
            let lock_name = lock_namec.to_string();
            let  break_flag_renewal = break_flag.clone();
            let  break_flag_work = break_flag.clone();
            let break_flag_single = break_flag.clone();

            println!("{} : 加锁成功", lock_namec);

            // 创建续期任务并保持其Handle
            let renewal_handle = tokio::spawn({
                let raft_clone = raft_clone.clone();
                let lock_name = lock_name.clone();
                async move {

                    let mut interval = tokio::time::interval(Duration::from_secs(50));
                    loop {
                        let break_flag;
                        {
                            break_flag = break_flag_renewal.read().await.load(std::sync::atomic::Ordering::SeqCst);
                        }
                        if break_flag{
                            break;
                        }
                        interval.tick().await;
                        {
                            match raft_clone.renewal(&lock_name, 60).await {
                                Ok(_) => println!("{} 续期成功", lock_name),
                                Err(e) => println!("{} 续期失败: {}", lock_name, e),
                            }
                        }

                    }
                }
            });

            // 创建工作处理任务
            let work_handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    let break_flag;
                    {
                        break_flag = break_flag_work.read().await.load(std::sync::atomic::Ordering::SeqCst);
                    }
                    if break_flag{
                        break;
                    }

                    interval.tick().await;
                    println!("{} ------------->处理数据...", lock_name);
                    tokio::time::sleep(Duration::from_secs(10)).await;

                    // break;
                }
            });

            // 监控任务状态
            tokio::spawn(async move {
                tokio::select! {
                _ = renewal_handle => {
                        println!("续期任务意外结束");

                    },
                _ = work_handle => {
                        println!("工作处理任务结束");

                    },
            }
                // 这里可以添加清理逻辑
                break_flag_single.write().await.store(true, std::sync::atomic::Ordering::Relaxed);
                // raft.un_lock(lock_namec).await.unwrap();

            });
        }
        tokio::time::sleep(Duration::from_secs(50)).await;
    }




    //
    //
    // loop {
    //     let res;
    //     {
    //         res = raft
    //             .write()
    //             .await
    //             .lock(lock_name, 300)
    //             .await
    //             .unwrap_or_default();
    //     }
    //     if res == 1 {
    //         let raft = raft.clone();
    //         async move {
    //             let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3));
    //             let mut interval1 = tokio::time::interval(tokio::time::Duration::from_secs(1));
    //             loop {
    //                 select! {
    //                     _ =  interval.tick() => {
    //                         let res;
    //                         {
    //                             res = raft.read().await.renewal(lock_name,10).await
    //                         }
    //                         match res{
    //                             Ok(res1) => {
    //                                 println!("{} 续期成功：{}",lock_name,res1 );
    //                             },
    //                             Err(e) => {
    //                                 println!("{} 续期失败：{}",lock_name ,e);
    //                             }
    //                         }
    //                     }
    //
    //                     _ =  interval1.tick() => {
    //                         tokio::time::sleep(Duration::from_secs(50)).await;
    //                         println!("lock_name : {} 处理数据---------->",lock_name );
    //
    //                     }
    //                 }
    //             }
    //         }
    //         .await;
    //         println!("{} : 加锁成功 {:?}", lock_name, res);
    //     }else {
    //         // println!("当前存在锁未释放：{:#?}",raft.read().await.get_members().await.unwrap())
    //     }
    //     tokio::time::sleep(Duration::from_secs(5)).await;
    // }
}

async fn basic_test() {
    let mut raft = Raft::new("test".to_string(), 2, 2);
    let res = raft.lock("local2", 300).await.unwrap();
    println!("local2-{:?}", res);
    let res = raft.lock("local1", 30).await.unwrap();
    println!("local1-{:?}", res);
    let res = raft.lock("local4", 3).await.unwrap();
    println!("local4-{:?}", res);
    tokio::time::sleep(Duration::from_secs(40)).await;
    let res = raft.lock("local3", 3).await.unwrap();
    println!("local3-{:?}", res);

    // 续期
    let res = raft.renewal("local2", 300).await.unwrap();
    println!("renewal local2-{:?}", res);

    let res = raft.renewal("local2", 300).await.unwrap();
    println!("renewal local2-{:?}", res);

    //获取全部key
    let res = raft.get_members().await.unwrap();
    println!("un_lock_all local2-{:?}", res);

    // 删除key
    let res = raft.un_lock("local2").await.unwrap();
    println!("un_lock local2-{:?}", res);

    let res = raft.un_lock("local2").await.unwrap();
    println!("un_lock local2-{:?}", res);

    //删除全部
    let res = raft.un_lock_all("test").await.unwrap();
    println!("un_lock_all local2-{:?}", res);
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
