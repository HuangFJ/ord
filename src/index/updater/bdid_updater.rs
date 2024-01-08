use super::inscription_updater::{Flotsam, Origin};
use super::*;
use crate::{
  bdid::entry::{InscriptionEvent, InscriptionEvents, OrdinalEventType},
  index::entry::Entry,
  index::entry::{InscriptionEntry, InscriptionEntryValue, InscriptionIdValue},
  unbound_outpoint, Index, InscriptionId, Result, SatPoint,
};
use bitcoin::{OutPoint, Transaction, Txid};
use rayon::prelude::*;
use redb::{ReadableTable, Table};
use std::{collections::HashMap, fs, path::PathBuf, time::Instant};

pub struct BdidUpdater<'a> {
  index: &'a Index,
  inscription_events: InscriptionEvents,
  pub inscription_new_satpoints: HashMap<InscriptionId, SatPoint>,
  event_db_path: PathBuf,
  height: u32,
  chain: crate::Chain,
  txid_to_tx: HashMap<Txid, Transaction>,
}

impl<'a> BdidUpdater<'a> {
  pub fn new(index: &'a Index, height: u32) -> Result<Self> {
    let event_db_path = crate::bdid::entry::event_db_path(&index.options)?;
    Ok(BdidUpdater {
      index,
      inscription_events: InscriptionEvents::new(Vec::new()),
      inscription_new_satpoints: HashMap::new(),
      event_db_path,
      height,
      chain: index.options.chain(),
      txid_to_tx: HashMap::new(),
    })
  }

  pub fn capture_transaction_inscriptions(
    &mut self,
    floating_inscriptions: &Vec<Flotsam>,
    txid: &Txid,
    is_coinbase: bool,
    id_to_sequence_number: &Table<InscriptionIdValue, u32>,
    sequence_number_to_entry: &Table<u32, InscriptionEntryValue>,
  ) -> Result {
    let start_time: Instant = Instant::now();

    floating_inscriptions
      .iter()
      .try_for_each(|flotsam| -> Result {
        let inscription_id_key = flotsam.inscription_id.store();

        let new_satpoint = self.inscription_new_satpoints.get(&flotsam.inscription_id);

        let new_satpoint = new_satpoint
          .unwrap_or(&SatPoint {
            outpoint: unbound_outpoint(),
            offset: 0,
          })
          .clone();

        match flotsam.origin {
          Origin::Old { old_satpoint } => {
            let sequence_number = id_to_sequence_number
              .get(inscription_id_key)?
              .unwrap()
              .value();

            let entry = InscriptionEntry::load(
              sequence_number_to_entry
                .get(sequence_number)?
                .unwrap()
                .value(),
            );

            // inscription is cursed or unbound, do not track
            if entry.inscription_number < 0 || Charm::Unbound.is_set(entry.charms) {
              return Ok(());
            }

            let receiver = match self.get_address_from_outpoint(&new_satpoint.outpoint) {
              Some((address, _is_coinbase)) => address,
              None => String::new(),
            };

            if is_coinbase {
              let event = self
                .inscription_events
                .events
                .par_iter_mut()
                .rev()
                .find_first(|event: &&mut InscriptionEvent| {
                  event.inscription_id == flotsam.inscription_id
                    && event.event_type == OrdinalEventType::Transferred
                });

              if let Some(e) = event {
                e.as_fee = true;
                e.output = new_satpoint;
                e.receiver = receiver;

                return Ok(()); // early return
              }
            }

            let sender = match self.get_address_from_outpoint(&old_satpoint.outpoint) {
              Some((address, _is_coinbase)) => address,
              None => String::new(),
            };

            self.inscription_events.events.push(InscriptionEvent {
              inscription_id: flotsam.inscription_id,
              event_type: OrdinalEventType::Transferred,
              input: Some(old_satpoint.outpoint),
              output: new_satpoint,
              txid: txid.to_owned(),
              unbound: false,
              as_fee: false,
              sender,
              receiver,
            });
          }
          Origin::New {
            cursed, unbound, ..
          } => {
            if unbound {
              return Ok(());
            }

            // coinbase fee create|cursed event should update as fee flag
            let as_fee = is_coinbase;

            let event_type = if cursed {
              OrdinalEventType::Cursed
            } else {
              OrdinalEventType::Created
            };

            let receiver = match self.get_address_from_outpoint(&new_satpoint.outpoint) {
              Some((address, _is_coinbase)) => address,
              None => String::new(),
            };

            if is_coinbase {
              let event = self
                .inscription_events
                .events
                .par_iter_mut()
                .rev()
                .find_first(|event| {
                  event.inscription_id == flotsam.inscription_id
                    && (event.event_type == OrdinalEventType::Created
                      || event.event_type == OrdinalEventType::Cursed)
                });

              if let Some(e) = event {
                e.as_fee = true;
                e.output = new_satpoint;
                e.receiver = receiver;

                return Ok(()); // early return
              }
            }

            // not found in coinbase or a new one, store created | cursed event
            self.inscription_events.events.push(InscriptionEvent {
              inscription_id: flotsam.inscription_id,
              event_type,
              input: None, // do not track input on inscribe
              output: new_satpoint,
              txid: txid.to_owned(),
              unbound,
              as_fee,
              sender: String::new(), // do not track input on inscribe
              receiver,
            });
          }
        };
        Ok(())
      })?;

    let end_time: Instant = Instant::now();
    let time_diff = end_time.duration_since(start_time);
    log::debug!(
      "cache_inscription_events took {}.{:03} seconds",
      time_diff.as_secs(),
      time_diff.subsec_millis()
    );
    Ok(())
  }

  pub fn commit(&mut self) -> Result {
    let start_time: Instant = Instant::now();

    let tmp_file: PathBuf = self.event_db_path.join(format!(".{}.tmp", self.height));
    let mut tmp_file_fd = fs::OpenOptions::new()
      .create(true)
      .truncate(true)
      .write(true)
      .read(true)
      .open(&tmp_file)?;

    self.inscription_events.to_parquet_file(&mut tmp_file_fd)?;
    tmp_file_fd.sync_all()?;

    let target_file = self.event_db_path.join(format!("{}.parquet", self.height));
    fs::rename(tmp_file, target_file)?;

    let end_time: Instant = Instant::now();
    let time_diff = end_time.duration_since(start_time);
    log::debug!(
      "save_inscription_events total {} took {}.{:03} seconds",
      self.inscription_events.events.len(),
      time_diff.as_secs(),
      time_diff.subsec_millis(),
    );
    Ok(())
  }

  fn get_address_from_outpoint(&mut self, outpoint: &OutPoint) -> Option<(String, bool)> {
    let tx = match self.txid_to_tx.get(&outpoint.txid) {
      Some(tx) => tx,
      None => {
        let Some(tx) = self.index.get_transaction(outpoint.txid).ok().flatten() else {
          return None;
        };
        let txid = tx.txid();
        self.txid_to_tx.insert(txid, tx);
        self.txid_to_tx.get(&txid).unwrap()
      }
    };

    let output = tx.output.iter().nth(outpoint.vout as usize);
    if output.is_none() {
      return Some((String::from(""), true)); // is coinbase
    }

    let output = output.unwrap();
    match self.chain.address_from_script(&output.script_pubkey) {
      Ok(address) => Some((address.to_string(), false)),
      Err(_) => None,
    }
  }
}
