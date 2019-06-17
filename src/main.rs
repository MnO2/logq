use clap::load_yaml;
use clap::App;

mod classic_load_balancer_log_record;
mod reader;

fn main() {
    let yaml = load_yaml!("cli.yml");
    let app_m = App::from_yaml(yaml).get_matches();

    match app_m.subcommand() {
        ("cat", Some(sub_m)) => {
            if let Some(files) = sub_m.values_of("files") {
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
            if let Some(columns) = sub_m.values_of("columns") {
                let filenames: Vec<&str> = columns.collect();
                println!("{:?}", filenames);
            } else {
                app_m.usage();
            }
        }
        _ => {
            app_m.usage();
        }
    }
}
