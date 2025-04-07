#[cfg(feature = "async")]
use crate::common::queue::QUEUED;

use crate::controller::redis::GLOBAL_REDIS_SCRIPT;

#[cfg(feature = "async")]
pub fn make_task_key(queue: &str, typ: &str, task_id: &str) -> String {
    //ZQL:{<qname>}:t:<task_id>
    format!("{}:{{{}}}:{{{}}}:{}", QUEUED, queue, typ, task_id)
}

#[cfg(feature = "async")]
pub fn make_queue(queue: &str, state: &str) -> String {
    //ZQL:{<qname>}:pending
    format!("{}:{{{}}}:{}", QUEUED, queue, state)
}

pub async fn get_sha(cmd: &str) -> Option<String> {
    GLOBAL_REDIS_SCRIPT
        .read()
        .await
        .get(cmd)
        .map(|res| res.to_string())
}

#[cfg(test)]
mod tests {
    use crate::common::func::{make_queue, make_task_key};

    #[test]
    fn test_make_task_key() {
        let res = make_queue("abc", "abc");
        println!("{}", res);

        let res = make_task_key("aa", "aa", "aa");
        println!("{}", res);
    }
}
