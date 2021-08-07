use std::collections::HashMap;
use std::{env};
use std::error::Error;
use serde::{Serialize, Deserialize};

fn main() {
    let args: Vec<String> = env::args().collect();
    let filename = &args[1];
    println!("{}", filename);
    let mut clients: HashMap<u16, ClientAccount> = HashMap::new();    
    stream_csv(filename, &mut clients).unwrap();
}


fn stream_csv(filename: &String, clients: &mut HashMap<u16, ClientAccount>) -> Result<(), Box<dyn Error>> {
    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::Reader::from_path(filename).unwrap();
    for result in rdr.deserialize() {
        let record : Record = result?;
        match record.action {
            Action::Deposit => handle_deposit(&record, clients),
            Action::Withdrawal => handle_withdrawal(&record, clients),
            Action::Dispute => handle_dispute(&record, clients),
            Action::Resolve => handle_resolve(&record, clients),
            Action::Chargeback => handle_chargeback(&record, clients),
        }        
        println!("{:?}", record);
    }
    Ok(())
}

// Increase clients available and total by deposit amount. If client account does not exist, create it.
fn handle_deposit(deposit: &Record, clients: &mut HashMap<u16, ClientAccount>) {
    let client_id = &deposit.client;
    let client = clients.get_mut(client_id);
    let deposit_amount = deposit.amount.unwrap();
    
    match client {
        Some(client) => {
            client.available += deposit_amount;
            client.total += deposit_amount;
        },
        None => {
            clients.insert(deposit.client, ClientAccount {
            client: deposit.client,
            available: deposit_amount,
            held: 0.0,
            total: deposit_amount,
            locked: false
        });}
    }
}

fn handle_withdrawal(withdrawal: &Record, clients: &mut HashMap<u16, ClientAccount>) {
    let client_id = &withdrawal.client;
    let client = clients.get_mut(client_id);
    let withdrawal_amount = withdrawal.amount.unwrap();
    match client {
        Some(client) => {
            if client.available - withdrawal_amount >= 0.0 {
                client.available -= withdrawal_amount;
                client.total -= withdrawal_amount;
            }
        },
        None => ()// Can't withdraw from a client account that does not exist
    }
}

fn handle_dispute(dispute: &Record, clients: &mut HashMap<u16, ClientAccount>) {
    
}

fn handle_chargeback(record: &Record, clients: &mut HashMap<u16, ClientAccount>) {
    todo!()
}
fn handle_resolve(resolve: &Record, clients: &mut HashMap<u16, ClientAccount>) {
    
}

#[derive(Debug, Deserialize)]
struct Record {
    #[serde(rename = "type")]
    action: Action,
    client: u16,
    #[serde(rename = "tx")]
    transaction: u32,
    amount: Option<f64>
}

#[derive(Serialize, Deserialize, Debug)]
enum Action {
    #[serde(rename = "deposit")]
    Deposit,
    #[serde(rename = "withdrawal")]
    Withdrawal,
    #[serde(rename = "dispute")]
    Dispute,
    #[serde(rename = "resolve")]
    Resolve,
    #[serde(rename = "chargeback")]
    Chargeback,
}

struct ClientAccount{
    // Client Id
    client: u16,
    // Total funds available for trading. available = total - held.
    available: f64,
    // Total funds held for dispute. held = total - available
    held: f64,
    // Total funds available or held. Total = available + held.
    total: f64,
    // Account is locked if charge back occurs
    locked: bool
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deposit_increments_correct_amount()
    {
        let mut clients: HashMap<u16, ClientAccount> = HashMap::new();
        let deposit = Record {
            client: 1,
            action: Action::Deposit,
            transaction: 1,
            amount: Some(20.0),
        };
        handle_deposit(&deposit, &mut clients);
        assert!(clients.contains_key(&1));
        assert_eq!(clients.get(&1).unwrap().available, 20.0 );
        assert_eq!(clients.get(&1).unwrap().total, 20.0 );
    }

    #[test]
    fn test_deposit_inserts_new_client()
    {
        let mut clients: HashMap<u16, ClientAccount> = HashMap::new();
        let deposit = Record {
            client: 1,
            action: Action::Deposit,
            transaction: 1,
            amount: Some(20.0),
        };
        handle_deposit(&deposit, &mut clients);
        assert!(clients.contains_key(&1));
        assert_eq!(clients.get(&1).unwrap().available, 20.0 );
        assert_eq!(clients.get(&1).unwrap().total, 20.0 );
    }

    #[test]
    fn test_withdrawal_subtracts_correct_amount()
    {
        let mut clients: HashMap<u16, ClientAccount> = HashMap::new();
        clients.insert(2, ClientAccount {
            client: 2,
            available: 100.0,
            total: 100.0,
            held: 0.0,
            locked: false,
        });
        let withdrawal = Record {
            client: 2,
            action: Action::Withdrawal,
            transaction: 1,
            amount: Some(20.0),
        };
        handle_withdrawal(&withdrawal, &mut clients);
        assert_eq!(clients.get(&2).unwrap().available, 80.0 );
        assert_eq!(clients.get(&2).unwrap().total, 80.0 );
    }

    #[test]
    fn test_withdrawal_fails_if_account_does_not_have_enough_funds()
    {
        let mut clients: HashMap<u16, ClientAccount> = HashMap::new();
        clients.insert(2, ClientAccount {
            client: 2,
            available: 100.0,
            total: 100.0,
            held: 0.0,
            locked: false,
        });
        let withdrawal = Record {
            client: 2,
            action: Action::Withdrawal,
            transaction: 1,
            amount: Some(250.0),
        };
        handle_withdrawal(&withdrawal, &mut clients);
        assert_eq!(clients.get(&2).unwrap().available, 100.0 );
        assert_eq!(clients.get(&2).unwrap().total, 100.0 );
    }
}