use crate::ast;
use crate::classic_load_balancer_log_field::ClassicLoadBalancerLogField;
use crate::datasource::reader::{Reader, ReaderError, StringRecord};
use std::collections::hash_map::HashMap;
use std::fs::File;
use std::io;
use std::result;
use std::str::FromStr;

pub type EvalResult = result::Result<(), EvalError>;

#[derive(Fail, Debug)]
pub enum EvalError {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "{}", _0)]
    Reader(#[cause] ReaderError),
}

impl From<io::Error> for EvalError {
    fn from(err: io::Error) -> EvalError {
        EvalError::Io(err)
    }
}

impl From<ReaderError> for EvalError {
    fn from(err: ReaderError) -> EvalError {
        EvalError::Reader(err)
    }
}

#[derive(Clone, Debug)]
pub struct Environment {
    pub filename: String,
}

pub fn eval(node: &ast::Node, env: &Environment) -> EvalResult {
    match node {
        ast::Node::Query(query) => eval_query(&query, env),
    }
}

enum QueryType {
    Select,
    Aggregation,
}

fn type_of_query(query: &ast::Query) -> QueryType {
    for field in query.fields.iter() {
        if let ast::Field::Expression(_) = field {
            return QueryType::Aggregation;
        }
    }

    QueryType::Select
}

fn eval_query(query: &ast::Query, env: &Environment) -> EvalResult {
    let file = File::open(&env.filename)?;
    let mut rdr = Reader::from_reader(file);
    let mut record = StringRecord::new();

    match type_of_query(query) {
        QueryType::Select => loop {
            let more_records = rdr.read_record(&mut record)?;
            if !more_records {
                break;
            } else {
                let mut result = String::new();
                for (k, field) in query.fields.iter().enumerate() {
                    if let ast::Field::Table(table_field) = field {
                        let f = ClassicLoadBalancerLogField::from_str(&table_field.name).unwrap();
                        if let Some(s) = record.get(f as usize) {
                            if k > 0 {
                                result.push_str(" ");
                            }
                            result.push_str(s);
                        };
                    }
                }

                println!("{:?}", result);
            }
        },
        QueryType::Aggregation => {
            let mut map: HashMap<String, (f64, usize)> = HashMap::new();

            loop {
                let more_records = rdr.read_record(&mut record)?;
                if !more_records {
                    break;
                } else {
                    for field in query.fields.iter() {
                        if let ast::Field::Expression(expression_field) = field {
                            if expression_field.func_call_expression.func_name == "avg" {
                                let value_column = expression_field
                                    .clone()
                                    .func_call_expression
                                    .arguments
                                    .first()
                                    .unwrap()
                                    .ident();
                                let partition_column = expression_field.partition_clause.clone().unwrap().field_name;
                                let value_column_enum = ClassicLoadBalancerLogField::from_str(&value_column).unwrap();
                                let partition_column_enum =
                                    ClassicLoadBalancerLogField::from_str(&partition_column).unwrap();

                                if let Some(s) = record.get(value_column_enum as usize) {
                                    let partition_key = record.get(partition_column_enum as usize).unwrap().to_string();
                                    let value: f64 = s.parse().unwrap();
                                    let entry = map.entry(partition_key).or_insert((0.0, 0));
                                    (*entry).0 += value;
                                    (*entry).1 += 1;
                                }
                            }
                        }
                    }
                }
            }

            for (partition_key, stats) in &map {
                println!("{:?} {:?}", partition_key, stats.0 / (stats.1 as f64));
            }
        }
    }

    Ok(())
}
