use log::*;
use clap::{App, Arg, ArgMatches, value_t_or_exit};
use solana_runtime::append_vec::AppendVec;
use std::path::Path;
use std::fs;

fn main() {
    solana_logger::setup();
    let matches = App::new("append-vec-tool")
        .about("Append vec tool")
        .version("version")
        .arg(
            Arg::with_name("file")
            .long("file")
            .index(1)
            .required(true)
            .takes_value(true)
            .help("append vec to read")
            )
        .get_matches();

    let path = value_t_or_exit!(matches, "file", String);
    info!("Path: {:?}", path);
    let attr = fs::metadata(path.clone()).unwrap();
    let mut append_vec = AppendVec::new_empty_map(attr.len() as usize, true);
    append_vec.set_file(Path::new(&path));
    for account in append_vec.accounts(0) {
        info!("account: {:?}", account);
    }
}
