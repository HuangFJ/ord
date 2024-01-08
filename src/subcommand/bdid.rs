use {
  super::*,
  crate::bdid::entry::{InscriptionEvents, OrdinalEventType},
  crate::bdid::response::{ApiError, RpcRequest, RpcResponse},
  crate::index::entry::InscriptionEntry,
  serde_json::Value,
  std::{str::FromStr, sync::Arc},
  tokio_util::sync::CancellationToken,
  warp::{Filter, Rejection, Reply},
};

#[derive(Parser, Debug)]
pub(crate) struct JServerArgs {
  #[arg(
    long,
    default_value = "127.0.0.1:8080",
    help = "Server address to listen on"
  )]
  address: String,
}

pub struct JsonRealServer {
  index: Arc<Index>,
  event_db_path: PathBuf,
}

impl JsonRealServer {
  pub(crate) fn new(options: Options) -> Result<Self> {
    let index = Arc::new(Index::open(&options)?);
    let event_db_path = crate::bdid::entry::event_db_path(&options)?;

    Ok(Self {
      index,
      event_db_path,
    })
  }

  fn index(&self) -> Arc<Index> {
    self.index.clone()
  }

  pub(crate) async fn get_block_count(&self) -> Result<u32, ApiError> {
    let index = self.index.clone();
    let block_count = tokio::task::spawn_blocking(move || index.block_count())
      .await
      .map_err(|err| {
        log::error!("get_block_count query index failed: {err}");
        ApiError::InternalError
      })?
      .map_err(|err| {
        log::error!("get_block_count query index failed: {err}");
        ApiError::InternalError
      })?;

    Ok(block_count)
  }

  pub(crate) async fn get_block_height(&self) -> Result<u32, ApiError> {
    let index = self.index.clone();
    let block_height = tokio::task::spawn_blocking(move || index.block_height())
      .await
      .map_err(|err| {
        log::error!("get_block_height query index failed: {err}");
        ApiError::InternalError
      })?
      .map_err(|err| {
        log::error!("get_block_height query index failed: {err}");
        ApiError::InternalError
      })?;

    Ok(block_height.map_or(0, |v| v.n()))
  }

  pub(crate) async fn get_block_hash(&self, height: u32) -> Result<BlockHash, ApiError> {
    let index = self.index.clone();
    let block_hash = tokio::task::spawn_blocking(move || index.block_hash(Some(height)))
      .await
      .map_err(|err| {
        log::error!("get_block_hash query index failed: {err}");
        ApiError::InternalError
      })?
      .map_err(|err| {
        log::error!("get_block_hash query index failed: {err}");
        ApiError::InternalError
      })?
      .ok_or(ApiError::BlockNotExists)?;

    Ok(block_hash)
  }

  pub(crate) async fn get_block_timestamp(&self, height: u32) -> Result<u32, ApiError> {
    let index = self.index.clone();
    let block_timestamp = tokio::task::spawn_blocking(move || index.block_time(Height(height)))
      .await
      .map_err(|err| {
        log::error!("get_block_timestamp query index failed: {err}");
        ApiError::InternalError
      })?
      .map_err(|err| {
        log::error!("get_block_timestamp query index failed: {err}");
        ApiError::InternalError
      })?;

    Ok(block_timestamp.unix_timestamp() as u32)
  }

  pub(crate) async fn get_transaction(&self, txid: Txid) -> Result<Option<Transaction>, ApiError> {
    let index = self.index.clone();
    let tx = tokio::task::spawn_blocking(move || index.get_transaction(txid))
      .await
      .map_err(|err| {
        log::error!("get_transaction query index failed: {err}");
        ApiError::InternalError
      })?
      .map_err(|err| {
        log::error!("get_transaction query index failed: {err}");
        ApiError::InternalError
      })?;

    Ok(tx)
  }

  pub(crate) async fn get_block_inscription_events(
    &self,
    height: u32,
  ) -> Result<Option<InscriptionEvents>, ApiError> {
    let path = self
      .event_db_path
      .clone()
      .join(format!("{}.parquet", height));
    let file = fs::OpenOptions::new()
      .read(true)
      .open(&path)
      .map_err(|err| {
        log::error!("get_transaction query index failed: {err}");
        ApiError::InternalError
      })?;
    let ev = InscriptionEvents::from_parquet_file(file).map_err(|err| {
      log::error!("get_transaction query index failed: {err}");
      ApiError::InternalError
    })?;

    Ok(Some(ev))
  }

  pub(crate) async fn get_inscriptions(
    &self,
    utxos: BTreeMap<OutPoint, Amount>,
  ) -> Result<BTreeMap<SatPoint, InscriptionId>, ApiError> {
    let index = self.index.clone();
    let inscriptions: BTreeMap<SatPoint, InscriptionId> =
      tokio::task::spawn_blocking(move || index.get_inscriptions(&utxos))
        .await
        .map_err(|err| {
          log::error!("get_inscriptions query index failed: {err}");
          ApiError::InternalError
        })?
        .map_err(|err| {
          log::error!("get_inscriptions query index failed: {err}");
          ApiError::InternalError
        })?;

    Ok(inscriptions)
  }

  pub(crate) async fn get_inscription_entry(
    &self,
    id: InscriptionId,
  ) -> Result<Option<InscriptionEntry>, ApiError> {
    let index = self.index.clone();
    let entry = tokio::task::spawn_blocking(move || index.get_inscription_entry(id))
      .await
      .map_err(|err| {
        log::error!("get_inscription_entry query index failed: {err}");
        ApiError::InternalError
      })?
      .map_err(|err| {
        log::error!("get_inscription_entry query index failed: {err}");
        ApiError::InternalError
      })?;

    Ok(entry)
  }

  pub(crate) async fn get_inscription_by_id(
    &self,
    id: InscriptionId,
  ) -> Result<Option<Inscription>, ApiError> {
    let index = self.index.clone();
    let entry = tokio::task::spawn_blocking(move || index.get_inscription_by_id(id))
      .await
      .map_err(|err| {
        log::error!("get_inscription_by_id query index failed: {err}");
        ApiError::InternalError
      })?
      .map_err(|err| {
        log::error!("get_inscription_by_id query index failed: {err}");
        ApiError::InternalError
      })?;

    Ok(entry)
  }

  pub(crate) async fn get_inscription_satpoint_by_id(
    &self,
    id: InscriptionId,
  ) -> Result<Option<SatPoint>, ApiError> {
    let index = self.index.clone();
    let entry: Option<SatPoint> =
      tokio::task::spawn_blocking(move || index.get_inscription_satpoint_by_id(id))
        .await
        .map_err(|err| {
          log::error!("get_inscription_satpoint_by_id query index failed: {err}");
          ApiError::InternalError
        })?
        .map_err(|err| {
          log::error!("get_inscription_satpoint_by_id query index failed: {err}");
          ApiError::InternalError
        })?;

    Ok(entry)
  }
}

pub(crate) fn run(jserver_args: JServerArgs, jserver: JsonRealServer) -> SubcommandResult {
  Runtime::new()?.block_on(async {
    let jserver = Arc::new(jserver);
    let addr = std::net::SocketAddr::from_str(&jserver_args.address).expect("addr parse err");

    let cors = warp::cors()
      .allow_any_origin()
      .allow_methods(vec!["GET", "POST", "OPTIONS"]);

    let shutdown_token = CancellationToken::new();
    let cloned_shutdown_token = shutdown_token.clone();
    let (_, server) = warp::serve(create_handle(jserver.clone()).with(cors))
      .bind_with_graceful_shutdown(addr, async move {
        cloned_shutdown_token.cancelled().await;
      });

    let server_handle = tokio::spawn(server);

    let index_clone = jserver.index();
    let sync_update_interval = match std::env::var("SYNC_UPDATE_INTERVAL")
      .ok()
      .map(|x| x.parse::<u64>().ok())
      .flatten()
    {
      None => 60,
      Some(x) => {
        if x < 60 {
          60
        } else {
          x
        }
      }
    };
    let update_handle = thread::spawn(move || loop {
      if SHUTTING_DOWN.load(atomic::Ordering::Relaxed) {
        break;
      }

      log::info!("try to update blocks");
      match index_clone.update() {
        Ok(_) => log::warn!("update blocks"),
        Err(error) => log::warn!("update blocks failed: {error}"),
      }

      _ = thread::sleep(Duration::from_secs(sync_update_interval));
    });
    INDEXER.lock().unwrap().replace(update_handle);

    let cloned_shutdown_token = shutdown_token.clone();
    loop {
      if SHUTTING_DOWN.load(atomic::Ordering::Relaxed) {
        _ = cloned_shutdown_token.cancel();
        _ = server_handle.await;

        println!("index shutdown");
        return Ok(Box::new(Empty {}) as Box<dyn Output>);
      }

      tokio::time::sleep(Duration::from_millis(1000)).await;
    }
  })
}

fn create_handle(
  ctx: Arc<JsonRealServer>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
  let health = warp::path("health").map(|| format!("ok"));

  let handle = warp::post()
    .and(warp::path::end())
    .and(warp::any().map(move || ctx.clone()))
    .and(warp::body::bytes())
    .and_then(handle_request);

  let log = warp::log("jserver::api");
  health.or(handle).with(log)
}

async fn handle_request(
  ctx: Arc<JsonRealServer>,
  body: bytes::Bytes,
) -> Result<impl Reply, Rejection> {
  let req_string = String::from_utf8_lossy(&body);

  let request: RpcRequest = match serde_json::from_slice::<RpcRequest>(&body) {
    Ok(val) => val,
    Err(err) => {
      log::error!("handle_request serde_json::from_slice failed: {err} {req_string}");
      return Ok(
        warp::reply::with_status("".to_string(), warp::http::StatusCode::BAD_REQUEST)
          .into_response(),
      );
    }
  };

  let mut response = RpcResponse {
    id: request.id,
    ..Default::default()
  };

  let resp = match request.method.as_str() {
    get_block_count::NAME => get_block_count::handle(ctx, request.params).await,
    get_block_height::NAME => get_block_height::handle(ctx, request.params).await,
    get_block_hash::NAME => get_block_hash::handle(ctx, request.params).await,
    get_block_inscription_events::NAME => {
      get_block_inscription_events::handle(ctx, request.params).await
    }
    get_inscription_entry::NAME => get_inscription_entry::handle(ctx, request.params).await,
    get_inscription_content::NAME => get_inscription_content::handle(ctx, request.params).await,
    get_inscription_satpoint::NAME => get_inscription_satpoint::handle(ctx, request.params).await,
    get_inscription_output::NAME => get_inscription_output::handle(ctx, request.params).await,
    get_output_inscriptions::NAME => get_output_inscriptions::handle(ctx, request.params).await,
    get_output_info::NAME => get_output_info::handle(ctx, request.params).await,
    _ => {
      log::info!("method[{}] not supported", request.method);
      Err(ApiError::InvalidMethod)
    }
  };
  match resp {
    Ok(data) => response.result = Some(serde_json::to_value(data).unwrap()),
    Err(err) => response.error = Some(err.to_rpc_error()),
  }

  Ok(warp::reply::json(&response).into_response())
}

mod get_block_count {
  use super::*;

  pub const NAME: &'static str = "block.count";

  #[derive(Deserialize)]
  struct Req {}

  #[derive(Default, Serialize)]
  struct Rsp {
    pub value: u32,
  }

  pub async fn handle(
    ctx: Arc<JsonRealServer>,
    _value: Value,
  ) -> Result<Box<dyn erased_serde::Serialize>, ApiError> {
    let mut rsp = Rsp::default();
    rsp.value = ctx.get_block_count().await?;

    Ok(Box::new(rsp))
  }
}

mod get_block_height {
  use super::*;

  pub const NAME: &'static str = "block.height";

  #[derive(Deserialize)]
  struct Req {}

  #[derive(Default, Serialize)]
  struct Rsp {
    pub value: u32,
  }

  pub async fn handle(
    ctx: Arc<JsonRealServer>,
    _value: Value,
  ) -> Result<Box<dyn erased_serde::Serialize>, ApiError> {
    let mut rsp = Rsp::default();
    let block_height = ctx.get_block_height().await?;

    rsp.value = block_height;

    Ok(Box::new(rsp))
  }
}

mod get_block_hash {
  use super::*;

  pub const NAME: &'static str = "block.hash";

  #[derive(Deserialize)]
  struct Req {
    pub height: u32,
  }

  #[derive(Serialize)]
  struct Rsp {
    pub hash: BlockHash,
  }

  pub async fn handle(
    ctx: Arc<JsonRealServer>,
    value: Value,
  ) -> Result<Box<dyn erased_serde::Serialize>, ApiError> {
    let req: Req = serde_json::from_value::<Req>(value).map_err(|err| {
      log::debug!("{NAME} parse params failed: {err}");
      ApiError::InvalidArgument
    })?;

    let mut rsp = Rsp {
      hash: BlockHash::all_zeros(),
    };
    let block_hash = ctx.get_block_hash(req.height).await?;

    rsp.hash = block_hash;

    Ok(Box::new(rsp))
  }
}

mod get_block_inscription_events {
  use super::*;
  use std::collections::HashMap;

  pub const NAME: &'static str = "block.inscription_events";

  #[derive(Deserialize)]
  struct Req {
    pub height: u32,
  }

  #[derive(Serialize)]
  struct Event {
    pub inscription_id: InscriptionId, // its inscription id (never change once inscribed)
    pub inscription_number: i32,
    pub event_type: OrdinalEventType,
    pub from: Option<String>,
    pub to: String,
    pub unbound: bool,
    pub as_fee: bool,
    pub input: Option<OutPoint>,
    pub output: SatPoint,
    pub txid: Txid,
  }

  #[derive(Serialize)]
  struct Rsp {
    pub height: u32,
    pub hash: BlockHash,
    pub timestamp: u32,
    pub events: Vec<Event>,
  }

  pub async fn handle(
    ctx: Arc<JsonRealServer>,
    value: Value,
  ) -> Result<Box<dyn erased_serde::Serialize>, ApiError> {
    let req: Req = serde_json::from_value::<Req>(value).map_err(|err| {
      log::debug!("{NAME} parse params failed: {err}");
      ApiError::InvalidArgument
    })?;

    if req.height > u32::MAX {
      return Err(ApiError::InvalidArgument);
    }

    let hash = ctx.get_block_hash(req.height).await?;

    let timestamp = ctx.get_block_timestamp(req.height).await?;

    let ev = ctx.get_block_inscription_events(req.height).await?;

    if ev.is_none() {
      return Ok(Box::new(Rsp {
        height: req.height as u32,
        hash,
        timestamp,
        events: Vec::default(),
      }));
    }
    let events = ev.unwrap().events; // never panic
    if events.len() == 0 {
      return Ok(Box::new(Rsp {
        height: req.height as u32,
        hash,
        timestamp,
        events: Vec::default(),
      }));
    }

    let mut number_cache: HashMap<InscriptionId, i32> = std::collections::HashMap::new();

    for e in events.iter() {
      if let Some(_) = number_cache.get(&e.inscription_id) {
        continue;
      }

      let entry_rsp = ctx.get_inscription_entry(e.inscription_id).await?;
      match entry_rsp {
        None => {
          log::error!(
            "{NAME} inscription {} number is not saved",
            e.inscription_id
          );
          return Err(ApiError::InternalError);
        }
        Some(inscription_entry) => {
          number_cache.insert(e.inscription_id, inscription_entry.inscription_number);
        }
      }
    }

    let rsp_events: Vec<Event> = events
      .iter()
      .map(|e| Event {
        from: Some(e.sender.clone()),
        to: e.receiver.clone(),
        inscription_number: number_cache.get(&e.inscription_id).unwrap().clone(), // do not fail here
        unbound: e.unbound,
        as_fee: e.as_fee,
        output: e.output,
        event_type: e.event_type,
        input: e.input,
        inscription_id: e.inscription_id,
        txid: e.txid,
      })
      .collect();

    Ok(Box::new(Rsp {
      height: req.height as u32,
      hash,
      timestamp,
      events: rsp_events,
    }))
  }
}

mod get_inscription_entry {
  use super::*;

  pub const NAME: &'static str = "inscription.enry";

  #[derive(Deserialize)]
  struct Req {
    pub txid: Txid,
    pub index: u32,
  }

  #[derive(Default, Serialize)]
  struct Rsp {
    pub fee: u64,
    pub height: u32,
    pub number: i32,
    pub sat: Option<Sat>,
    pub timestamp: u32,
  }

  pub async fn handle(
    ctx: Arc<JsonRealServer>,
    value: Value,
  ) -> Result<Box<dyn erased_serde::Serialize>, ApiError> {
    let req = serde_json::from_value::<Req>(value).map_err(|err| {
      log::debug!("{NAME} parse params failed: {err}");
      ApiError::InvalidArgument
    })?;

    let entry = ctx
      .get_inscription_entry(InscriptionId {
        txid: req.txid,
        index: req.index,
      })
      .await?;

    let rsp = match entry {
      Some(entry) => Rsp {
        fee: entry.fee,
        height: entry.height,
        number: entry.inscription_number,
        sat: entry.sat,
        timestamp: entry.timestamp,
      },
      None => Rsp::default(),
    };

    Ok(Box::new(rsp))
  }
}

mod get_inscription_content {
  use super::*;

  pub const NAME: &'static str = "inscription.content";

  #[derive(Deserialize)]
  struct Req {
    pub txid: Txid,
    pub index: u32,
  }

  #[derive(Default, Serialize)]
  struct Rsp {
    body: Option<Vec<u8>>,
    content_type: String,
  }

  pub async fn handle(
    ctx: Arc<JsonRealServer>,
    value: Value,
  ) -> Result<Box<dyn erased_serde::Serialize>, ApiError> {
    let req = serde_json::from_value::<Req>(value).map_err(|err| {
      log::debug!("{NAME} parse params failed: {err}");
      ApiError::InvalidArgument
    })?;

    let entry = ctx
      .get_inscription_by_id(InscriptionId {
        txid: req.txid,
        index: req.index,
      })
      .await?;

    let rsp = match &entry {
      Some(e) => Rsp {
        body: e.body.clone(),
        content_type: match e.content_type() {
          Some(v) => String::from(v),
          None => String::new(),
        },
      },
      None => Rsp::default(),
    };

    Ok(Box::new(rsp))
  }
}

mod get_inscription_satpoint {
  use super::*;

  pub const NAME: &'static str = "inscription.satpoint";

  #[derive(Deserialize)]
  struct Req {
    pub txid: Txid,
    pub index: u32,
  }

  #[derive(Default, Serialize)]
  struct Rsp {
    pub outpoint: OutPoint,
    pub offset: u64,
  }

  pub async fn handle(
    ctx: Arc<JsonRealServer>,
    value: Value,
  ) -> Result<Box<dyn erased_serde::Serialize>, ApiError> {
    let req = serde_json::from_value::<Req>(value).map_err(|err| {
      log::debug!("{NAME} parse params failed: {err}");
      ApiError::InvalidArgument
    })?;

    let entry = ctx
      .get_inscription_satpoint_by_id(InscriptionId {
        txid: req.txid,
        index: req.index,
      })
      .await?;

    let rsp = match &entry {
      Some(e) => Rsp {
        outpoint: e.outpoint,
        offset: e.offset,
      },
      None => Rsp::default(),
    };

    Ok(Box::new(rsp))
  }
}

mod get_inscription_output {
  use super::*;

  pub const NAME: &'static str = "inscription.output";

  #[derive(Deserialize)]
  struct Req {
    pub txid: Txid,
    pub index: u32,
  }

  #[derive(Serialize)]
  struct Rsp {
    pub value: u64,
    pub script_pubkey: ScriptBuf,
  }

  pub async fn handle(
    ctx: Arc<JsonRealServer>,
    value: Value,
  ) -> Result<Box<dyn erased_serde::Serialize>, ApiError> {
    let req = serde_json::from_value::<Req>(value).map_err(|err| {
      log::debug!("{NAME} parse params failed: {err}");
      ApiError::InvalidArgument
    })?;

    let satpoint = ctx
      .get_inscription_satpoint_by_id(InscriptionId {
        txid: req.txid,
        index: req.index,
      })
      .await?;

    if satpoint.is_none() {
      return Err(ApiError::ItemNotFound);
    }

    let satpoint = satpoint.unwrap();

    let output = ctx
      .get_transaction(satpoint.outpoint.txid)
      .await?
      .unwrap()
      .output
      .into_iter()
      .nth(satpoint.outpoint.vout.try_into().unwrap());

    match &output {
      Some(e) => Ok(Box::new(Rsp {
        value: e.value,
        script_pubkey: e.script_pubkey.clone(),
      })),
      None => Err(ApiError::ItemNotFound),
    }
  }
}

mod get_output_inscriptions {
  use super::*;

  pub const NAME: &'static str = "utxo.inscriptions";

  #[derive(Deserialize, Copy, Clone)]
  struct OutputOutPoint {
    pub txid: Txid,
    pub vout: u32,
  }

  #[derive(Deserialize)]
  struct Req {
    pub outpoints: Vec<OutputOutPoint>,
  }

  #[derive(Serialize)]
  struct OutputInscriptionIds {
    pub txid: Txid,
    pub vout: u32,
    pub inscription_ids: Vec<InscriptionId>,
  }

  #[derive(Default, Serialize)]
  struct Rsp {
    pub output_inscriptions_list: Vec<OutputInscriptionIds>,
  }

  pub async fn handle(
    ctx: Arc<JsonRealServer>,
    value: Value,
  ) -> Result<Box<dyn erased_serde::Serialize>, ApiError> {
    let req = serde_json::from_value::<Req>(value).map_err(|err| {
      log::debug!("{NAME} parse params failed: {err}");
      ApiError::InvalidArgument
    })?;

    let mut utxos = BTreeMap::new();
    utxos.extend(req.outpoints.iter().map(|utxo| {
      let outpoint = OutPoint::new(utxo.txid, utxo.vout);
      (outpoint, Amount::ZERO)
    }));

    let all_inscriptions = ctx.get_inscriptions(utxos).await?;

    let mut outpoint_inscriptions: BTreeMap<OutPoint, Vec<InscriptionId>> = BTreeMap::new();
    for (k, v) in all_inscriptions {
      match outpoint_inscriptions.get_mut(&k.outpoint) {
        Some(vs) => vs.push(v),
        None => {
          outpoint_inscriptions.insert(k.outpoint, vec![v]);
        }
      }
    }

    let rsp_content: Vec<OutputInscriptionIds> = req
      .outpoints
      .iter()
      .map(|utxo| {
        let outpoint = OutPoint::new(utxo.txid, utxo.vout);
        match outpoint_inscriptions.get(&outpoint) {
          Some(vs) => OutputInscriptionIds {
            txid: utxo.txid,
            vout: utxo.vout,
            inscription_ids: vs.clone(),
          },
          None => OutputInscriptionIds {
            txid: utxo.txid,
            vout: utxo.vout,
            inscription_ids: vec![],
          },
        }
      })
      .collect();

    Ok(Box::new(Rsp {
      output_inscriptions_list: rsp_content,
    }))
  }
}

mod get_output_info {
  use super::*;

  pub const NAME: &'static str = "utxo.info";

  #[derive(Deserialize)]
  struct Req {
    pub txid: Txid,
    pub vout: u32,
  }

  #[derive(Serialize)]
  struct Rsp {
    pub txid: Txid,
    pub vout: u32,
    pub value: u64,
    pub script_pubkey: ScriptBuf,
  }

  pub async fn handle(
    ctx: Arc<JsonRealServer>,
    value: Value,
  ) -> Result<Box<dyn erased_serde::Serialize>, ApiError> {
    let req = serde_json::from_value::<Req>(value).map_err(|err| {
      log::debug!("{NAME} parse params failed: {err}");
      ApiError::InvalidArgument
    })?;

    let output: Option<TxOut> = ctx
      .get_transaction(req.txid)
      .await?
      .unwrap()
      .output
      .into_iter()
      .nth(req.vout as usize);

    if output.is_none() {
      return Err(ApiError::ItemNotFound);
    }

    let output = output.unwrap();

    Ok(Box::new(Rsp {
      txid: req.txid,
      vout: req.vout,
      value: output.value,
      script_pubkey: output.script_pubkey,
    }))
  }
}
