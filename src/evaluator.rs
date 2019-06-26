use std::result;
use std::fmt;
use std::io;
use crate::ast;
use crate::string_record::StringRecord;
use std::fs::File;
use crate::reader;
use crate::classic_load_balancer_log_field::ClassicLoadBalancerLogField;
use std::str::FromStr;

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

enum QueryType {
    Select,
    Aggregation
}

fn type_of_query(query: &ast::Query) -> QueryType {
    for field in query.fields.iter() {
        match field {
            ast::Field::Expression(expression_field) => {
                return QueryType::Aggregation;
            },
            _ => {}
        }
    }

    return QueryType::Select;
}

fn eval_query(query: &ast::Query, env: &Environment) -> EvalResult {
    let mut file = File::open(&env.filename)?;
    let mut rdr = reader::Reader::from_reader(file);
    let mut record = StringRecord::new();

    match type_of_query(query) {
        QueryType::Select => {
            while let more_records = rdr.read_record(&mut record)? {
                if !more_records {
                    break;
                } else {
                    let mut result = String::new();
                    for (k, field) in query.fields.iter().enumerate() {
                        match field {
                            ast::Field::Table(table_field) => {
                                let f = ClassicLoadBalancerLogField::from_str(&table_field.name).unwrap();
                                if let Some(s) = record.get(f as usize) {
                                    if k > 0 {
                                        result.push_str(" ");
                                    }
                                    result.push_str(s);
                                };
                            },
                            _ => {}
                        }
                    }

                    println!("{:?}", result);
                }
            }
        },
        QueryType::Aggregation => {

        }
    }



    Ok(())
}
