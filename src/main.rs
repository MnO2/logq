#[macro_use]
extern crate failure;

mod execution;
mod ast;
mod classic_load_balancer_log_field;
mod evaluator;
mod lexer;
mod parser;
mod datasource;
mod token;

use clap::load_yaml;
use clap::App;

fn main() {
    let yaml = load_yaml!("cli.yml");
    let app_m = App::from_yaml(yaml).get_matches();

    match app_m.subcommand() {
        ("select", Some(sub_m)) => {
            if let (Some(query_str), Some(filename)) = (sub_m.value_of("query"), sub_m.value_of("file_to_select")) {
                match parser::parse(&query_str) {
                    Ok(node) => {
                        let env = evaluator::Environment {
                            filename: filename.to_string(),
                        };

                        match evaluator::eval(&node, &env) {
                            Ok(_) => {}
                            Err(e) => {
                                println!("{:?}", e);
                            }
                        }
                    }
                    Err(errs) => {
                        for e in errs {
                            eprintln!("{}", e);
                        }
                    }
                }
            } else {
                sub_m.usage();
            }
        }
        _ => {
            app_m.usage();
        }
    }
}
