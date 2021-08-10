# Transaction Processor
### Building:


```bash
cargo build --release
```

### Running:

A csv file is required as a command line argument. Any outout is written to stdout.
```bash
cargo run transactions.csv
```

###  Unit tests

This repo contains unit tests to verify the code handles transactions correctly under different circumstances. To run these use the below command:
```bash
cargo test
```

