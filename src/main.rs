use core::panic;
use std::env;
mod transaction_processor;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        panic!("No argument found for transactions file");
    }
    let filename = &args[1];
    let mut tx_processor = transaction_processor::TransactionProcessor::new();
    tx_processor
        .stream_csv(filename)
        .expect("Error reading csv file");
    tx_processor
        .print_client_accounts()
        .expect("Error printing status of client accounts");
}
