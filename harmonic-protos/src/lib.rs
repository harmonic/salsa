pub mod auth {
    tonic::include_proto!("auth");
}

pub mod shared {
    tonic::include_proto!("shared");
}

pub mod packet {
    tonic::include_proto!("packet");
}

pub mod bundle {
    tonic::include_proto!("bundle");
}

/// Auction house (block engine) gRPC types.
pub mod auction {
    tonic::include_proto!("block_engine");
}

/// TPU proxy (relayer) gRPC types.
pub mod tpu_proxy {
    tonic::include_proto!("relayer");
}
