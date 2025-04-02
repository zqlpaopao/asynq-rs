use crate::controller::task::Task;
use std::fmt::Debug;

pub trait TaskHandlerTrait: Send + Sync + Debug {
    type Item;
    fn handle_task(&mut self, task: Task) -> Self::Item;
}
