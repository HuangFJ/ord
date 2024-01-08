use {
  serde::{Deserialize, Serialize},
  serde_json::Value,
};

#[derive(Default, Serialize, Deserialize)]
pub struct RpcRequest {
  pub id: Option<Value>,
  pub method: String,
  pub params: Value,
}

#[derive(Default, Serialize)]
pub struct RpcResponse {
  pub id: Option<Value>,
  pub error: Option<RpcError>,
  pub result: Option<Value>,
}

#[derive(Serialize)]
pub struct RpcError {
  pub code: i64,
  pub message: String,
}

pub enum ApiError {
  InvalidMethod,
  InvalidArgument,
  ItemNotFound,
  BlockNotExists,
  InternalError,
}

impl ApiError {
  pub fn to_rpc_error(&self) -> RpcError {
    let (code, message) = match self {
      ApiError::InternalError => (1000, format!("internal server error")),
      ApiError::InvalidMethod => (2000, format!("method does not exist/is not available")),
      ApiError::ItemNotFound => (2001, format!("item not found")),
      ApiError::BlockNotExists => (2002, format!("block not exists")),
      ApiError::InvalidArgument => (3, format!("invalid argument")),
    };
    RpcError { code, message }
  }
}
