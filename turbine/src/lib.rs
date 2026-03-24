#![cfg(feature = "agave-unstable-api")]
#![allow(clippy::arithmetic_side_effects)]

mod addr_cache;

pub mod broadcast_stage;

pub mod cluster_nodes;

pub mod retransmit_stage;

pub mod sigverify_shreds;

#[macro_use]
extern crate log;

#[macro_use]
extern crate solana_metrics;

#[cfg(test)]
#[macro_use]
extern crate assert_matches;

use {smallvec::SmallVec, std::net::SocketAddr};

/// Addresses to forward shreds to in addition to normal turbine propagation.
pub type ShredReceiverAddresses = SmallVec<[SocketAddr; 5]>;
