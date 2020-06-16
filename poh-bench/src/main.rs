use clap::{crate_description, crate_name, value_t, App, Arg};
use solana_ledger::entry::{create_ticks, init_poh, EntrySlice, EntryVerificationStatus};
use solana_measure::measure::Measure;
use solana_sdk::hash::hash;

fn main() {
    solana_logger::setup();

    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .arg(
            Arg::with_name("max_num_entries")
                .long("max-num-entries")
                .takes_value(true)
                .value_name("SIZE")
                .help("Number of entries."),
        )
        .arg(
            Arg::with_name("start_num_entries")
                .long("start-num-entries")
                .takes_value(true)
                .value_name("SIZE")
                .help("Packets per chunk"),
        )
        .arg(
            Arg::with_name("hashes_per_tick")
                .long("hashes-per-tick")
                .takes_value(true)
                .value_name("SIZE")
                .help("hashes per tick"),
        )
        .arg(
            Arg::with_name("num_transactions_per_entry")
                .long("num-transactions-per-entry")
                .takes_value(true)
                .value_name("NUM")
                .help("Skip transaction sanity execution"),
        )
        .arg(
            Arg::with_name("iterations")
                .long("iterations")
                .takes_value(true)
                .help("Number of iterations"),
        )
        .arg(
            Arg::with_name("num_threads")
                .long("num-threads")
                .takes_value(true)
                .help("Number of threads"),
        )
        .get_matches();

    let max_num_entries = value_t!(matches, "max_num_entries", u64).unwrap_or(64);
    let start_num_entries = value_t!(matches, "start_num_entries", u64).unwrap_or(max_num_entries);
    let iterations = value_t!(matches, "iterations", usize).unwrap_or(10);
    let hashes_per_tick = value_t!(matches, "hashes_per_tick", u64).unwrap_or(10_000);
    let start_hash = hash(&[1, 2, 3, 4]);
    let ticks = create_ticks(max_num_entries, hashes_per_tick, start_hash);
    let mut num_entries = start_num_entries as usize;
    init_poh();
    while num_entries <= max_num_entries as usize {
        let mut time = Measure::start("time");
        for _ in 0..iterations {
            assert_eq!(
                ticks[..num_entries]
                    .verify_cpu_generic(&start_hash)
                    .status(),
                EntryVerificationStatus::Success
            );
        }
        time.stop();
        println!(
            "{},cpu_generic,{}",
            num_entries,
            time.as_us() / iterations as u64
        );

        let mut time = Measure::start("time");
        for _ in 0..iterations {
            assert_eq!(
                ticks[..num_entries]
                    .verify_cpu_x86_simd(&start_hash, 8)
                    .status(),
                EntryVerificationStatus::Success
            );
        }
        time.stop();
        println!(
            "{},cpu_simd_avx2,{}",
            num_entries,
            time.as_us() / iterations as u64
        );

        let mut time = Measure::start("time");
        for _ in 0..iterations {
            assert_eq!(
                ticks[..num_entries]
                    .verify_cpu_x86_simd(&start_hash, 16)
                    .status(),
                EntryVerificationStatus::Success
            );
        }
        time.stop();
        println!(
            "{},cpu_simd_avx512,{}",
            num_entries,
            time.as_us() / iterations as u64
        );

        println!("");
        num_entries *= 2;
    }
}
