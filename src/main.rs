mod classic_load_balancer_log_record;
mod reader;

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
        ("cat", Some(sub_m)) => {
            if let Some(files) = sub_m.values_of("files_to_cat") {
                let filenames: Vec<&str> = files.collect();
                println!("{:?}", filenames);
            } else {
                app_m.usage();
            }
        }
        ("search", Some(sub_m)) => {
            if let Some(keyword) = sub_m.value_of("keyword") {
                println!("{:?}", keyword);
            } else {
                app_m.usage();
            }
        }
        ("select", Some(sub_m)) => {
            if let (Some(columns), Some(filename)) = (sub_m.value_of("columns"), sub_m.value_of("file_to_select")) {
		        let mut file = File::open(filename)?;
                let mut rdr = Reader::from_reader(file);
                let mut record = ClassicLoadBalancerLogRecord::empty();

                let column_strs: Vec<&str> = columns.split(",").collect();

                loop {
                    if let more_records = rdr.read_record(&mut record)? {
                        if !more_records {
                            break;
                        } else {
                            let mut result = String::new();
                            for c in &column_strs {
                                match c {
                                    &"timestamp" => { result.push_str(&record.timestamp) },
                                    _ => {}
                                }
                            } 

                            println!("{:?}", result)
                        }
                    } else {
                        break;
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
