use std::result;
use std::fmt;
use std::io;
use crate::ast;
use crate::classic_load_balancer_log_record::ClassicLoadBalancerLogRecord;
use std::fs::File;
use crate::reader;

pub type EvalResult = result::Result<(), EvalError>;

#[derive(Debug)]
pub struct EvalError {
    pub message: String
}

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for EvalError {
    fn description(&self) -> &str {
        &self.message
    }
}

impl From<io::Error> for EvalError {
    fn from(err: io::Error) -> EvalError {
        EvalError { message: String::from("") }
    }
}

impl From<reader::Error> for EvalError {
    fn from(err: reader::Error) -> EvalError {
        EvalError { message: String::from("") }
    }
}

#[derive(Clone, Debug)]
pub struct Environment {
    pub filename: String
}

pub fn eval(node: &ast::Node, env: &Environment) -> EvalResult {
    match node {
        ast::Node::Query(query) => eval_query(&query, env)
    }
}

fn eval_query(query: &ast::Query, env: &Environment) -> EvalResult {
    let mut file = File::open(&env.filename)?;
    let mut rdr = reader::Reader::from_reader(file);
    let mut record = ClassicLoadBalancerLogRecord::empty();

    println!("{:?}", &env.filename);
    while let more_records = rdr.read_record(&mut record)? {
        if !more_records {
            break;
        } else {
            let mut result = String::new();
            result.push_str(&record.timestamp);

            println!("{:?}", result)
        }
    }

    Ok(())
}