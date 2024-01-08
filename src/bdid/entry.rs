use crate::{InscriptionId, Options, SatPoint};
use anyhow::{anyhow, bail, Context, Result};
use bitcoin::{OutPoint, Txid};
use polars::{
  prelude::{
    AnyValue, DataFrame, ParquetCompression, ParquetReader, ParquetWriter, SerReader, ZstdLevel,
  },
  series::Series,
};
use serde::{Deserialize, Serialize};
use std::{
  fs::{self, File},
  path::PathBuf,
  str::FromStr,
};

#[derive(Debug, Serialize, Deserialize, PartialEq, Copy, Clone)]
pub enum OrdinalEventType {
  Created,
  Transferred,
  Cursed,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct InscriptionEvent {
  pub inscription_id: InscriptionId,
  pub event_type: OrdinalEventType,
  pub input: Option<OutPoint>,
  pub output: SatPoint,
  pub txid: Txid,
  pub unbound: bool,
  pub as_fee: bool,
  pub sender: String,
  pub receiver: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct InscriptionEvents {
  pub events: Vec<InscriptionEvent>,
}

impl InscriptionEvents {
  pub(crate) fn new(events: Vec<InscriptionEvent>) -> Self {
    InscriptionEvents { events }
  }

  pub(crate) fn to_parquet_file(&self, file: &mut File) -> Result<()> {
    let mut inscription_id_series = Series::from_iter(
      self
        .events
        .iter()
        .filter(|ev| !ev.unbound)
        .map(|event| event.inscription_id.to_string()),
    );
    let mut event_type_series = Series::from_iter(
      self
        .events
        .iter()
        .filter(|ev| !ev.unbound)
        .map(|event| format!("{:?}", event.event_type)),
    );
    let mut input_series =
      Series::from_iter(self.events.iter().filter(|ev| !ev.unbound).map(|event| {
        event
          .input
          .and_then(|input: OutPoint| Some(input.to_string()))
          .unwrap_or(String::new())
      }));
    let mut output_series = Series::from_iter(
      self
        .events
        .iter()
        .filter(|ev| !ev.unbound)
        .map(|event| event.output.to_string()),
    );
    let mut txid_series = Series::from_iter(
      self
        .events
        .iter()
        .filter(|ev| !ev.unbound)
        .map(|event| event.txid.to_string()),
    );
    let mut unbound_series = Series::from_iter(
      self
        .events
        .iter()
        .filter(|ev| !ev.unbound)
        .map(|event| event.unbound),
    );
    let mut as_fee_series = Series::from_iter(
      self
        .events
        .iter()
        .filter(|ev| !ev.unbound)
        .map(|event| event.as_fee),
    );
    let mut sender_series = Series::from_iter(
      self
        .events
        .iter()
        .filter(|ev| !ev.unbound)
        .map(|event| event.sender.to_string()),
    );
    let mut receiver_series = Series::from_iter(
      self
        .events
        .iter()
        .filter(|ev| !ev.unbound)
        .map(|event| event.receiver.to_string()),
    );

    inscription_id_series.rename("inscription_id");
    event_type_series.rename("event_type");
    input_series.rename("input");
    output_series.rename("output");
    txid_series.rename("txid");
    unbound_series.rename("unbound");
    as_fee_series.rename("as_fee");
    sender_series.rename("sender");
    receiver_series.rename("receiver");

    let mut event_df = DataFrame::new(vec![
      inscription_id_series,
      event_type_series,
      input_series,
      output_series,
      txid_series,
      unbound_series,
      as_fee_series,
      sender_series,
      receiver_series,
    ])?;

    ParquetWriter::new(file)
      .with_statistics(true)
      .with_compression(ParquetCompression::Zstd(Some(ZstdLevel::try_new(3)?)))
      .finish(&mut event_df)?;

    Ok(())
  }

  pub(crate) fn from_parquet_file(file: File) -> anyhow::Result<Self> {
    let events_df: DataFrame = ParquetReader::new(file).finish()?;

    let count = events_df.column("inscription_id")?.len();
    let mut events: Vec<InscriptionEvent> = Vec::new();

    for i in 0..count {
      let inscription_id =
        to_string(&events_df.column("inscription_id")?.get(i)?).context("no inscription_id")?;

      let event_type =
        to_string(&events_df.column("event_type")?.get(i)?).context("no event_type")?;
      let input = to_string(&events_df.column("input")?.get(i)?).context("no intput")?;
      let output = to_string(&events_df.column("output")?.get(i)?).context("no output")?;
      let txid = to_string(&events_df.column("txid")?.get(i)?).context("no txid")?;
      let unbound = to_bool(&events_df.column("unbound")?.get(i)?);
      let as_fee = to_bool(&events_df.column("as_fee")?.get(i)?);
      let sender = to_string(&events_df.column("sender")?.get(i)?).context("no sender")?;
      let receiver = to_string(&events_df.column("receiver")?.get(i)?).context("no receiver")?;

      events.push(InscriptionEvent {
        inscription_id: InscriptionId::from_str(&inscription_id)?,
        txid: Txid::from_str(&txid).map_err(|err| anyhow!("invalid txid: {}", err))?,
        input: match input.as_str() {
          "" => None,
          input => {
            Some(OutPoint::from_str(input).map_err(|err| anyhow!("invalid input: {}", err))?)
          }
        },
        output: SatPoint::from_str(&output).map_err(|err| anyhow!("invalid output: {}", err))?,
        sender: String::from(&sender),
        receiver: match receiver.as_str() {
          s => String::from(s),
        },
        event_type: match event_type.as_str() {
          "Created" => OrdinalEventType::Created,
          "Transferred" => OrdinalEventType::Transferred,
          "Cursed" => OrdinalEventType::Cursed,
          _ => Err(anyhow!("not supported ordinal event type"))?,
        },
        as_fee,
        unbound,
      })
    }

    Ok(InscriptionEvents { events })
  }
}

fn to_bool<'a>(v: &AnyValue<'a>) -> bool {
  if let AnyValue::Boolean(b) = v {
    *b
  } else {
    false
  }
}

fn to_string<'a>(v: &AnyValue<'a>) -> Option<String> {
  if let Some(s) = v.get_str() {
    Some(s.to_string())
  } else {
    None
  }
}

pub fn event_db_path(options: &Options) -> Result<PathBuf> {
  let path = if let Some(path) = &options.index {
    path.clone()
  } else {
    options.data_dir().join("index.redb")
  };

  let event_db_path = path.parent().unwrap().join("inscription_events");
  if let Err(err) = fs::create_dir_all(&event_db_path) {
    bail!(
      "failed to create inscription event dir `{}`: {err}",
      event_db_path.display()
    );
  }

  Ok(event_db_path)
}
