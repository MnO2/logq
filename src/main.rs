mod classic_load_balancer_log_record;
mod reader;
mod parser;
mod evaluator;
mod ast;

use clap::load_yaml;
use clap::App;
use std::result;
use std::fs::File;
use std::io;
use reader::Reader;
use classic_load_balancer_log_record::ClassicLoadBalancerLogRecord;

fn main() -> result::Result<(), reader::Error> {
    let yaml = load_yaml!("cli.yml");
    let app_m = App::from_yaml(yaml).get_matches();

    match app_m.subcommand() {
        ("select", Some(sub_m)) => {
            if let (Some(query_str), Some(filename)) = (sub_m.value_of("query_str"), sub_m.value_of("file_to_select")) {
                match parser::parse(&query_str) {
                    Ok(node) => {
                        evaluator::eval(&node, &filename);
                    },
                    Err(e) => {

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

    Ok(())
}
