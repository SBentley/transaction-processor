use std::{env};

use crate::transaction_processor::TransactionProcessor;
mod transaction_processor;

fn main() {
    let args: Vec<String> = env::args().collect();
    let filename = &args[1];
    println!("{}", filename);
    let mut transaction_processor = TransactionProcessor::new();
    transaction_processor
        .stream_csv(filename)
        .expect("Error reading csv file");
    transaction_processor.output_client_accounts().unwrap();
}

