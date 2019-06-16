use clap::App;
use clap::load_yaml;

fn main() {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches();

    if let Some(_matches) = matches.subcommand_matches("cat") {
        println!("executing command {}", matches.subcommand_name().unwrap());
    } else if let Some(_) = matches.subcommand_matches("partition") {
        println!("executing command {}", matches.subcommand_name().unwrap());
    } else if let Some(_) = matches.subcommand_matches("index") {
        println!("executing command {}", matches.subcommand_name().unwrap());
    } else if let Some(_) = matches.subcommand_matches("search") {
        println!("executing command {}", matches.subcommand_name().unwrap());
    } else if let Some(_) = matches.subcommand_matches("select") {
        println!("executing command {}", matches.subcommand_name().unwrap());
    } else {
        println!("{}", matches.usage()); 
    }
}
