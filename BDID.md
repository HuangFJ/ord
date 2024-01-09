### env
`SYNC_UPDATE_INTERVAL`

### Cargo.toml
```toml
warp = { version = "0.3.2", features = ["tls"] }
erased-serde = "0.3"
bytes = "1.4.0"
polars = { version = "0.33.2", features = [
    "polars-io",
    "parquet",
    "dtype-struct",
] }
rayon = "1.5.1"
```

### src/lib.rs
```rust
pub mod bdid;
```

### src/subcommand.rs
```rust
pub mod bdid;

pub(crate) enum Subcommand {
  ...
  #[command(about = "BDID server commands")]
  Bdid(bdid::JServerArgs),
}

impl Subcommand {
  pub(crate) fn run(self, options: Options) -> SubcommandResult {
    match self {
      ...
      Self::Bdid(jserver_args) => bdid::run(jserver_args, bdid::JsonRealServer::new(options)?),
    }
  }
}
```

### src/index/updater.rs
```rust
mod bdid_updater;

fn index_block() {
  ...
  let mut bdid_updater = bdid_updater::BdidUpdater::new(self.index, self.height)?;
  ...
  bdid_updater.commit()?;
}
```

### src/index/updater/inscription_updater.rs
```rust
fn update_inscription_location(){
  ...
  bdid_updater
    .inscription_new_satpoints
    .insert(flotsam.inscription_id, SatPoint::load(satpoint));
}

fn index_envelopes() {
  ...
  bdid_updater.capture_transaction_inscriptions(
    &floating_inscriptions,
    &txid,
    is_coinbase,
    self.id_to_sequence_number,
    self.sequence_number_to_entry,
  )?;
}
```

### /etc/systemd/system/ord.service
```shell
Environment="RUST_LOG=info"
Environment="POSTGRES_URL=postgres://user:pwd@127.0.0.1:5432/did?sslmode=disable"
ExecStart=ord --config ord.yaml --rpc-url http://127.0.0.1:8332 --data-dir data --index-sats --index-transactions bdid --address 0.0.0.0:8080
```