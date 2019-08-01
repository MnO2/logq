use std::result;

pub(crate) type ExecutionResult<T> = result::Result<T, ExecutionError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum ExecutionError {
    #[fail(display = "Stream Error")]
    Stream
}

fn run_plan() -> ExecutionResult<()> {
    
}