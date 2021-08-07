use std::collections::HashMap;
use std::{env};
use std::error::Error;
use serde::{Serialize, Deserialize};

fn main() {
    let args: Vec<String> = env::args().collect();
    let filename = &args[1];
    println!("{}", filename);
    let mut transaction_processor = TransactionProcessor::new();
    transaction_processor.stream_csv(filename).expect("Error reading csv file");
    
}

struct TransactionProcessor {
    accounts: HashMap<u16, ClientAccount>,
    transaction_log : HashMap<u32, Record>,
}

impl TransactionProcessor {
    fn new() -> TransactionProcessor {
        TransactionProcessor {
            accounts: HashMap::new(),
            transaction_log: HashMap::new()
        }
    }

    fn stream_csv(&mut self, filename: &String) -> Result<(), Box<dyn Error>> {
        // Build the CSV reader and iterate over each record.
        let mut rdr = csv::Reader::from_path(filename).unwrap();
        for result in rdr.deserialize() {
            let record : Record = result?;
            println!("{:?}", record);
            match record.action {
                Action::Deposit => self.handle_deposit(record),
                Action::Withdrawal => self.handle_withdrawal(record),
                Action::Dispute => self.handle_dispute(record),
                Action::Resolve => self.handle_resolve(record),
                Action::Chargeback => self.handle_chargeback(&record),
            }
        }
        Ok(())
    }
    // Increase clients available and total by deposit amount. If client account does not exist, create it.
    fn handle_deposit(&mut self, deposit: Record) {
        let client_id = &deposit.client;
        let client = self.accounts.get_mut(client_id);
        let deposit_amount = deposit.amount.unwrap();
        match client {
            Some(client) => {
                client.available += deposit_amount;
                client.total += deposit_amount;
            },
            None => {
                self.accounts.insert(deposit.client, ClientAccount {
                    client: deposit.client,
                    available: deposit_amount,
                    held: 0.0,
                    total: deposit_amount,
                    locked: false
                });}
        }
        self.transaction_log.insert(deposit.transaction, deposit);
    }
    fn handle_withdrawal(&mut self, withdrawal: Record) {
        let client_id = &withdrawal.client;
        let account = self.accounts.get_mut(client_id);
        let withdrawal_amount = withdrawal.amount.unwrap();
        if let Some(account) = account {
            if account.available - withdrawal_amount >= 0.0 {
                account.available -= withdrawal_amount;
                account.total -= withdrawal_amount;
            }
        }
        self.transaction_log.insert(withdrawal.transaction, withdrawal);
    }
    
    fn handle_dispute(&mut self, dispute: Record) {
        let client_id = &dispute.client;
        let account = self.accounts.get_mut(client_id);
        if let Some(account) = account {
            if let Some(tx) = self.transaction_log.get(&dispute.transaction) {
                account.held += tx.amount.unwrap();
                account.available -= tx.amount.unwrap();
            }
        }
    }
    
    fn handle_resolve(&mut self, resolve: Record) {
        let client_id = resolve.client;
        let account = self.accounts.get_mut(&client_id);
        if let Some(account) = account {
            if let Some(tx) = self.transaction_log.get(&resolve.transaction) {
                account.held -= tx.amount.unwrap();
                account.available += tx.amount.unwrap();
            }
        }
    }
    fn handle_chargeback(&mut self, chargeback: &Record) {
        let client_id = chargeback.client;
        let account = self.accounts.get_mut(&client_id);
        if let Some(account) = account {
            if let Some(tx) = self.transaction_log.get(&chargeback.transaction) {
                account.held -= tx.amount.unwrap();
                account.total -= tx.amount.unwrap();
                account.locked = true;
            }
        }
    }
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



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deposit_increments_correct_amount()
    {
        let mut tx_processor = TransactionProcessor::new();
        tx_processor.accounts.insert(1, ClientAccount {
            client: 1,
            available: 100.0,
            total: 100.0,
            held: 0.0,
            locked: false,
        });
        let deposit = Record {
            client: 1,
            action: Action::Deposit,
            transaction: 1,
            amount: Some(20.0),
        };
        tx_processor.handle_deposit(deposit);
        assert!(tx_processor.accounts.contains_key(&1));
        assert_eq!(tx_processor.accounts.get(&1).unwrap().available, 120.0 );
        assert_eq!(tx_processor.accounts.get(&1).unwrap().total, 120.0 );
    }

    #[test]
    fn test_deposit_inserts_new_client()
    {
        let mut tx_processor = TransactionProcessor::new();
        let deposit = Record {
            client: 1,
            action: Action::Deposit,
            transaction: 1,
            amount: Some(20.0),
        };
        tx_processor.handle_deposit(deposit);
        assert!(tx_processor.accounts.contains_key(&1));
        assert_eq!(tx_processor.accounts.get(&1).unwrap().available, 20.0 );
        assert_eq!(tx_processor.accounts.get(&1).unwrap().total, 20.0 );
    }

    #[test]
    fn test_withdrawal_subtracts_correct_amount()
    {
        let mut tx_processor = TransactionProcessor::new();
        tx_processor.accounts.insert(2, ClientAccount {
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
        tx_processor.handle_withdrawal(withdrawal);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().available, 80.0 );
        assert_eq!(tx_processor.accounts.get(&2).unwrap().total, 80.0 );
    }

    #[test]
    fn test_withdrawal_fails_if_account_does_not_have_enough_funds()
    {
        let mut tx_processor = TransactionProcessor::new();
        tx_processor.accounts.insert(2, ClientAccount {
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
        tx_processor.handle_withdrawal(withdrawal);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().available, 100.0 );
        assert_eq!(tx_processor.accounts.get(&2).unwrap().total, 100.0 );
    }
}