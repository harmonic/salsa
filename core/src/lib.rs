#![cfg_attr(feature = "frozen-abi", feature(min_specialization))]
#![allow(clippy::arithmetic_side_effects)]
#![recursion_limit = "2048"]
//! The `solana` library implements the Solana high-performance blockchain architecture.
//! It includes a full Rust implementation of the architecture (see
//! [Validator](validator/struct.Validator.html)) as well as hooks to GPU implementations of its most
//! paralellizable components (i.e. [SigVerify](sigverify/index.html)).  It also includes
//! command-line tools to spin up validators and a Rust library
//!

pub mod admin_rpc_post_init;
pub mod banking_simulation;
pub mod banking_stage;
pub mod banking_trace;
pub mod bundle_stage;
pub mod cluster_info_vote_listener;
pub mod cluster_slots_service;
pub mod commitment_service;
pub mod completed_data_sets_service;
pub mod consensus;
pub mod cost_update_service;
pub mod drop_bank_service;
pub mod fetch_stage;
pub mod forwarding_stage;
pub mod gen_keys;
pub mod immutable_deserialized_bundle;
mod mock_alpenglow_consensus;
pub mod next_leader;
pub mod optimistic_confirmation_verifier;
pub mod packet_bundle;
pub mod proxy;
pub mod repair;
pub mod replay_stage;
mod result;
pub mod sample_performance_service;
mod shred_fetch_stage;
pub mod sigverify;
pub mod sigverify_stage;
pub mod snapshot_packager_service;
pub mod staked_nodes_updater_service;
pub mod stats_reporter_service;
pub mod system_monitor_service;
pub mod tip_manager;
pub mod tpu;
mod tpu_entry_notifier;
pub mod tvu;
pub mod unfrozen_gossip_verified_vote_hashes;
pub mod validator;
mod vortexor_receiver_adapter;
pub mod vote_simulator;
pub mod voting_service;
pub mod warm_quic_cache_service;
pub mod window_service;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate solana_metrics;

#[cfg_attr(feature = "frozen-abi", macro_use)]
#[cfg(feature = "frozen-abi")]
extern crate solana_frozen_abi_macro;

#[cfg(test)]
#[macro_use]
extern crate assert_matches;

use {
    bytes::Bytes,
    solana_packet::{Meta, PacketFlags, PACKET_DATA_SIZE},
    solana_perf::packet::BytesPacket,
    std::{
        cmp::min,
        net::{IpAddr, Ipv4Addr},
    },
};

const UNKNOWN_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));

// NOTE: last profiled at around 180ns
pub fn proto_packet_to_packet(p: jito_protos::proto::packet::Packet) -> BytesPacket {
    let copy_len = min(PACKET_DATA_SIZE, p.data.len());
    let mut packet = BytesPacket::new(
        Bytes::copy_from_slice(&p.data[0..copy_len]),
        Meta::default(),
    );

    if let Some(meta) = p.meta {
        packet.meta_mut().size = meta.size as usize;
        packet.meta_mut().addr = meta.addr.parse().unwrap_or(UNKNOWN_IP);
        packet.meta_mut().port = meta.port as u16;
        if let Some(flags) = meta.flags {
            if flags.simple_vote_tx {
                packet.meta_mut().flags.insert(PacketFlags::SIMPLE_VOTE_TX);
            }
            if flags.forwarded {
                packet.meta_mut().flags.insert(PacketFlags::FORWARDED);
            }
            if flags.repair {
                packet.meta_mut().flags.insert(PacketFlags::REPAIR);
            }
            if flags.from_staked_node {
                packet
                    .meta_mut()
                    .flags
                    .insert(PacketFlags::FROM_STAKED_NODE);
            }
        }
    }
    packet
}

pub(crate) mod scheduler_synchronization {
    //! mevanoxx TODO: maybe move this
    //!
    //! Synchronize whole block and vanilla schedulers.
    //!
    //! Every slot, there are two stages: delegation and fallback.
    //! During delegation stage, the block scheduler await a whole block.
    //! If a whole block is received by the end of the stage,

    use std::sync::atomic::{AtomicU64, Ordering};

    /// Module private state. Shared with block & vanilla schedulers.
    /// Initialized to sentinel value of u64::MAX.
    static LAST_SLOT_SCHEDULED: AtomicU64 = AtomicU64::new(SENTINEL);
    const SENTINEL: u64 = u64::MAX;

    pub fn last_slot_scheduled() -> u64 {
        LAST_SLOT_SCHEDULED.load(Ordering::Acquire)
    }

    /// If vanilla should schedule, the internal private atomic is
    /// updated so that the block scheduler does not schedule.
    ///
    /// This fn is not idempotent so nonnull return value should
    /// be cached.
    ///
    /// None => no but decision not final (ie not yet at least)
    /// Some(true) => yes and decision for this slot is final
    /// Some(false) => no and decision for this slot is final
    pub(crate) fn vanilla_should_schedule(
        current_slot: u64,
        in_delegation_period: bool,
    ) -> Option<bool> {
        if in_delegation_period {
            return None;
        }

        let result = LAST_SLOT_SCHEDULED.fetch_update(
            Ordering::Release,
            Ordering::Acquire,
            |last_slot_scheduled| {
                let update = match last_slot_scheduled.cmp(&current_slot) {
                    // No longer in delegation period and last slot scheduled was in the past => update
                    std::cmp::Ordering::Less => Some(current_slot),
                    // Something has been scheduled for this slot => no update
                    std::cmp::Ordering::Equal => None,

                    // Edge case at slot boundary or sentinel value
                    std::cmp::Ordering::Greater => {
                        // If sentinel value, similar to Less but for startup case
                        let is_sentinel_value = last_slot_scheduled == SENTINEL;
                        if is_sentinel_value {
                            return Some(current_slot);
                        }

                        // Otherwise, some weird edge case (don't schedule)
                        None
                    }
                };

                update
            },
        );

        if let Ok(old) = result {
            info!("DEVIN DEBUG: vanilla updated slot from {old} to {current_slot}");
        } else {
            info!("DEVIN DEBUG: vanilla unable to take lock for {current_slot}");
        }

        Some(result.is_ok())
    }

    /// If block should schedule, the internal private atomic is
    /// updated so that the vanilla scheduler does not schedule.
    ///
    /// This fn is not idempotent so nonnull return value should
    /// be cached.
    ///
    /// None => no but decision not final (ie not yet at least)
    /// Some(true) => yes and decision for this slot is final
    /// Some(false) => no and decision for this slot is final
    pub(crate) fn block_should_schedule(
        current_slot: u64,
        in_delegation_period: bool,
    ) -> Option<bool> {
        if !in_delegation_period {
            return None;
        }

        let did_update_atomic = LAST_SLOT_SCHEDULED
            .fetch_update(
                Ordering::Release,
                Ordering::Acquire,
                |last_slot_scheduled| {
                    let update = match last_slot_scheduled.cmp(&current_slot) {
                        // In delegation period and last slot scheduled was in the past => update
                        std::cmp::Ordering::Less => Some(current_slot),
                        // Something has been scheduled for this slot => no update.
                        // We shouldn't ever hit this branch so lets log it...
                        std::cmp::Ordering::Equal => {
                            info!("mevanoxx: unexpectedly hit Equal branch");
                            None
                        }

                        // Edge case at slot boundary or sentinel value
                        std::cmp::Ordering::Greater => {
                            // If sentinel value, similar to Less but for startup case
                            let is_sentinel_value = last_slot_scheduled == SENTINEL;
                            if is_sentinel_value {
                                return Some(current_slot);
                            }

                            // Otherwise, some weird edge case (don't schedule)
                            None
                        }
                    };

                    update
                },
            )
            .is_ok();

        info!("mevanoxx: block did_update_atomic {did_update_atomic}");

        if did_update_atomic {
            info!("mevanoxx: block updated slot to {current_slot}");
        }

        Some(did_update_atomic)
    }

    /// If block failed, we should revert and give vanilla a chance
    /// updated so that the vanilla scheduler does not schedule.
    pub(crate) fn block_failed(current_slot: u64) -> Option<bool> {
        info!("mevanoxx: block_failed {current_slot}");

        let did_update_atomic = LAST_SLOT_SCHEDULED
            .fetch_update(
                Ordering::Release,
                Ordering::Acquire,
                |last_slot_scheduled| {
                    info!("mevanoxx: block_failed fetch_update {last_slot_scheduled}");

                    let update = match last_slot_scheduled.cmp(&current_slot) {
                        // Still in same slot => revert
                        //
                        // doesn't have to be actual last slot, just one that is less than (or sentinel)
                        std::cmp::Ordering::Equal => Some(current_slot.wrapping_sub(1)),

                        // New slot => don't revert (this includes sentinel case, which is unreachable)
                        // Less => already reverted, likely duplicate calls while reverting block, ignore
                        std::cmp::Ordering::Greater | std::cmp::Ordering::Less => None,
                    };

                    info!("mevanoxx: block_failed fetch_update update {update:?}");

                    update
                },
            )
            .is_ok();

        info!("mevanoxx: block_failed did_update_atomic {did_update_atomic}");

        if did_update_atomic {
            info!("mevanoxx: block reverted in slot {current_slot}");
        }
        Some(did_update_atomic)
    }
}
