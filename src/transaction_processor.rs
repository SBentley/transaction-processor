use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::error::Error;
use std::io;

pub struct TransactionProcessor {
    /// Keep track of all client accounts and associated values
    accounts: HashMap<u16, ClientAccount>,
    /// Keep basic info on deposit and withdrawal transactions so that we can handle disputes/chargebacks
    transaction_log: HashMap<u32, Record>,
}

impl TransactionProcessor {
    pub fn new() -> TransactionProcessor {
        TransactionProcessor {
            accounts: HashMap::new(),
            transaction_log: HashMap::new(),
        }
    }

    pub fn stream_csv(&mut self, filename: &String) -> Result<(), Box<dyn Error>> {
        let mut rdr = csv::Reader::from_path(filename)
            .expect(format!("Unable to open {}", filename).as_str());

        for result in rdr.deserialize() {
            let record: Record = result?;
            match record.action {
                Action::Deposit => self.handle_deposit(record),
                Action::Withdrawal => self.handle_withdrawal(record),
                Action::Dispute => self.handle_dispute(record),
                Action::Resolve => self.handle_resolve(record),
                Action::Chargeback => self.handle_chargeback(record),
            }
        }
        Ok(())
    }

    // Increase clients available and total by deposit amount. If client account does not exist, create it.
    fn handle_deposit(&mut self, deposit: Record) {
        let client = self.accounts.get_mut(&deposit.client);
        let deposit_amount = deposit.amount.unwrap();
        match client {
            Some(client) => {
                client.available += deposit_amount;
                client.total += deposit_amount;
            }
            None => {
                self.accounts.insert(
                    deposit.client,
                    ClientAccount {
                        client: deposit.client,
                        available: deposit_amount,
                        held: 0.0,
                        total: deposit_amount,
                        locked: false,
                    },
                );
            }
        }
        self.transaction_log.insert(deposit.transaction, deposit);
    }

    fn handle_withdrawal(&mut self, withdrawal: Record) {
        let account = self.accounts.get_mut(&withdrawal.client);
        let withdrawal_amount = withdrawal
            .amount
            .expect("Withdrawal transaction did not have a value.");
        if let Some(account) = account {
            if account.available - withdrawal_amount >= 0.0 {
                account.available -= withdrawal_amount;
                account.total -= withdrawal_amount;
            }
        }
        self.transaction_log
            .insert(withdrawal.transaction, withdrawal);
    }

    fn handle_dispute(&mut self, dispute: Record) {
        let account = self.accounts.get_mut(&dispute.client);
        if let Some(account) = account {
            if let Some(tx) = self.transaction_log.get(&dispute.transaction) {
                account.held += tx
                    .amount
                    .expect("Transaction referenced in a dispute did not have a value.");
                account.available -= tx
                    .amount
                    .expect("Transaction referenced in a dispute did not have a value.");
            }
        }
    }

    fn handle_resolve(&mut self, resolve: Record) {
        let account = self.accounts.get_mut(&resolve.client);
        if let Some(account) = account {
            if let Some(tx) = self.transaction_log.get(&resolve.transaction) {
                account.held -= tx
                    .amount
                    .expect("Transaction referenced in a resolution did not have a value.");
                account.available += tx
                    .amount
                    .expect("Transaction referenced in a resolution did not have a value.");
            }
        }
    }

    fn handle_chargeback(&mut self, chargeback: Record) {
        let account = self.accounts.get_mut(&chargeback.client);
        if let Some(account) = account {
            if let Some(tx) = self.transaction_log.get(&chargeback.transaction) {
                account.held -= tx
                    .amount
                    .expect("Transaction referenced in a chargeback did not have a value.");
                account.total -= tx
                    .amount
                    .expect("Transaction referenced in a chargeback did not have a value.");
                account.locked = true;
            }
        }
    }

    pub fn print_client_accounts(&self) -> Result<(), Box<dyn Error>> {
        let mut writer = csv::Writer::from_writer(io::stdout());
        for (_, account) in &self.accounts {
            writer.serialize(account)?;
        }
        writer.flush()?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct Record {
    #[serde(rename = "type")]
    action: Action,
    client: u16,
    #[serde(rename = "tx")]
    transaction: u32,
    amount: Option<f32>,
}

#[derive(Serialize)]
struct ClientAccount {
    /// Client Id
    client: u16,
    /// Total funds available for trading. available = total - held.
    #[serde(serialize_with = "four_decimal_serializer")]
    available: f32,
    /// Total funds held for dispute. held = total - available
    #[serde(serialize_with = "four_decimal_serializer")]
    held: f32,
    /// Total funds available or held. Total = available + held.
    #[serde(serialize_with = "four_decimal_serializer")]
    total: f32,
    /// Account is locked if charge back occurs
    locked: bool,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
enum Action {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

fn four_decimal_serializer<S>(x: &f32, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(format!("{:.4}", x).as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deposit_increments_correct_amount() {
        // Arrange
        let mut tx_processor = TransactionProcessor::new();
        tx_processor.accounts.insert(
            1,
            ClientAccount {
                client: 1,
                available: 100.0,
                total: 100.0,
                held: 0.0,
                locked: false,
            },
        );
        let deposit = Record {
            client: 1,
            action: Action::Deposit,
            transaction: 1,
            amount: Some(20.0),
        };

        // Act
        tx_processor.handle_deposit(deposit);

        // Assert
        assert!(tx_processor.accounts.contains_key(&1));
        assert_eq!(tx_processor.accounts.get(&1).unwrap().available, 120.0);
        assert_eq!(tx_processor.accounts.get(&1).unwrap().total, 120.0);
    }

    #[test]
    fn test_deposit_inserts_new_client() {
        // Arrange
        let mut tx_processor = TransactionProcessor::new();
        let deposit = Record {
            client: 1,
            action: Action::Deposit,
            transaction: 1,
            amount: Some(20.0),
        };
        // Act
        tx_processor.handle_deposit(deposit);

        // Assert
        assert!(tx_processor.accounts.contains_key(&1));
        assert_eq!(tx_processor.accounts.get(&1).unwrap().available, 20.0);
        assert_eq!(tx_processor.accounts.get(&1).unwrap().total, 20.0);
    }

    #[test]
    fn test_withdrawal_subtracts_correct_amount() {
        // Arrange
        let mut tx_processor = TransactionProcessor::new();
        tx_processor.accounts.insert(
            2,
            ClientAccount {
                client: 2,
                available: 100.0,
                total: 100.0,
                held: 0.0,
                locked: false,
            },
        );
        let withdrawal = Record {
            client: 2,
            action: Action::Withdrawal,
            transaction: 1,
            amount: Some(20.0),
        };

        // Act
        tx_processor.handle_withdrawal(withdrawal);

        // Assert
        assert_eq!(tx_processor.accounts.get(&2).unwrap().available, 80.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().total, 80.0);
    }

    #[test]
    fn test_withdrawal_fails_if_account_does_not_have_enough_funds() {
        // Arrange
        let mut tx_processor = TransactionProcessor::new();
        tx_processor.accounts.insert(
            2,
            ClientAccount {
                client: 2,
                available: 100.0,
                total: 100.0,
                held: 0.0,
                locked: false,
            },
        );
        let withdrawal = Record {
            client: 2,
            action: Action::Withdrawal,
            transaction: 1,
            amount: Some(250.0),
        };

        // Act
        tx_processor.handle_withdrawal(withdrawal);

        // Assert
        assert_eq!(tx_processor.accounts.get(&2).unwrap().available, 100.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().total, 100.0);
    }

    #[test]
    fn test_dispute_ignores_dispute_for_non_existing_transaction() {
        // Arrange
        let mut tx_processor = TransactionProcessor::new();
        tx_processor.accounts.insert(
            2,
            ClientAccount {
                client: 2,
                available: 100.0,
                total: 100.0,
                held: 0.0,
                locked: false,
            },
        );
        let dispute = Record {
            client: 2,
            action: Action::Dispute,
            transaction: 1,
            amount: None,
        };

        // Act
        tx_processor.handle_dispute(dispute);

        // Assert
        assert_eq!(tx_processor.accounts.get(&2).unwrap().available, 100.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().total, 100.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().held, 0.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().locked, false);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().client, 2);
    }

    #[test]
    fn test_dispute_changes_available_and_held_values() {
        // Arrange
        let mut tx_processor = TransactionProcessor::new();
        tx_processor.accounts.insert(
            2,
            ClientAccount {
                client: 2,
                available: 100.0,
                total: 100.0,
                held: 0.0,
                locked: false,
            },
        );

        let withdrawal = Record {
            client: 2,
            action: Action::Withdrawal,
            transaction: 1,
            amount: Some(25.0),
        };
        let dispute = Record {
            client: 2,
            action: Action::Dispute,
            transaction: 1,
            amount: None,
        };
        tx_processor.transaction_log.insert(1, withdrawal);

        // Act
        tx_processor.handle_dispute(dispute);

        // Assert
        assert_eq!(tx_processor.accounts.get(&2).unwrap().available, 75.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().held, 25.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().total, 100.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().locked, false);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().client, 2);
    }

    #[test]
    fn test_resolve_reimburses_client() {
        // Arrange
        let mut tx_processor = TransactionProcessor::new();
        tx_processor.accounts.insert(
            2,
            ClientAccount {
                client: 2,
                available: 75.0,
                total: 100.0,
                held: 25.0,
                locked: false,
            },
        );
        let withdrawal = Record {
            client: 2,
            action: Action::Withdrawal,
            transaction: 1,
            amount: Some(25.0),
        };
        tx_processor.transaction_log.insert(1, withdrawal);
        let resolve = Record {
            action: Action::Resolve,
            client: 2,
            transaction: 1,
            amount: None,
        };

        // Act
        tx_processor.handle_resolve(resolve);

        // Assert
        assert_eq!(tx_processor.accounts.get(&2).unwrap().held, 0.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().available, 100.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().total, 100.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().locked, false);
    }

    #[test]
    fn test_resolve_ignores_resolve_for_non_existing_transaction() {
        // Arrange
        let mut tx_processor = TransactionProcessor::new();
        tx_processor.accounts.insert(
            2,
            ClientAccount {
                client: 2,
                available: 100.0,
                total: 100.0,
                held: 0.0,
                locked: false,
            },
        );
        let resolve = Record {
            client: 2,
            action: Action::Resolve,
            transaction: 1,
            amount: None,
        };

        // Act
        tx_processor.handle_resolve(resolve);

        // Assert
        assert_eq!(tx_processor.accounts.get(&2).unwrap().available, 100.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().total, 100.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().held, 0.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().locked, false);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().client, 2);
    }

    #[test]
    fn test_chargeback_ignores_chargeback_for_non_existing_transaction() {
        // Arrange
        let mut tx_processor = TransactionProcessor::new();
        tx_processor.accounts.insert(
            2,
            ClientAccount {
                client: 2,
                available: 100.0,
                total: 100.0,
                held: 0.0,
                locked: false,
            },
        );
        let chargeback = Record {
            client: 2,
            action: Action::Chargeback,
            transaction: 1,
            amount: None,
        };

        // Act
        tx_processor.handle_chargeback(chargeback);

        // Assert
        assert_eq!(tx_processor.accounts.get(&2).unwrap().available, 100.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().held, 0.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().locked, false);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().client, 2);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().total, 100.0);
    }

    #[test]
    fn test_chargeback_locks_account_and_changes_values() {
        // Arrange
        let mut tx_processor = TransactionProcessor::new();
        tx_processor.accounts.insert(
            2,
            ClientAccount {
                client: 2,
                available: 75.0,
                total: 100.0,
                held: 25.0,
                locked: false,
            },
        );
        let withdrawal = Record {
            client: 2,
            action: Action::Withdrawal,
            transaction: 1,
            amount: Some(25.0),
        };
        tx_processor.transaction_log.insert(1, withdrawal);
        let chargeback = Record {
            client: 2,
            action: Action::Resolve,
            transaction: 1,
            amount: None,
        };
        // Act
        tx_processor.handle_chargeback(chargeback);

        // Assert
        assert_eq!(tx_processor.accounts.get(&2).unwrap().available, 75.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().held, 0.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().locked, true);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().total, 75.0);
        assert_eq!(tx_processor.accounts.get(&2).unwrap().client, 2);
    }
}
