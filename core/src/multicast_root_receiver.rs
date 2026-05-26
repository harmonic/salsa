//! Validator-side wiring for the multicast root receiver feature.
//!
//! Owns the dedicated multicast destination constants and the
//! `MulticastShredCheckService` factory that watches kernel route state for
//! both the leader-broadcast and turbine-root multicast groups.

use {
    crate::multicast_shred_check_service::{
        MULTICAST_SHRED_ADDR_MAINNET, MULTICAST_SHRED_ADDR_TESTNET, MulticastShredCheckService,
    },
    arc_swap::ArcSwap,
    solana_cluster_type::ClusterType,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{Arc, atomic::AtomicBool},
    },
};

pub const MULTICAST_ROOT_SHRED_ADDR_MAINNET: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(233, 84, 178, 16)), 7733);

pub const MULTICAST_ROOT_SHRED_ADDR_TESTNET: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(233, 84, 178, 12)), 7733);

/// Returns the `(leader_broadcast, turbine_root)` multicast destinations for
/// a cluster, or `None` when the cluster has no multicast group.
pub fn addresses_for_cluster(cluster_type: ClusterType) -> Option<(SocketAddr, SocketAddr)> {
    match cluster_type {
        ClusterType::MainnetBeta => Some((
            MULTICAST_SHRED_ADDR_MAINNET,
            MULTICAST_ROOT_SHRED_ADDR_MAINNET,
        )),
        ClusterType::Testnet => Some((
            MULTICAST_SHRED_ADDR_TESTNET,
            MULTICAST_ROOT_SHRED_ADDR_TESTNET,
        )),
        _ => None,
    }
}

/// Builds both `MulticastShredCheckService` instances (leader broadcast +
/// turbine-root forwarding) for clusters that advertise a multicast group.
/// Returns an empty `Vec` when the feature is disabled by config or the
/// cluster type has no multicast destination.
pub fn spawn_shred_check_services(
    exit: Arc<AtomicBool>,
    cluster_type: ClusterType,
    enabled: bool,
    leader_receiver_address: Arc<ArcSwap<Option<SocketAddr>>>,
    root_receiver_address: Arc<ArcSwap<Option<SocketAddr>>>,
) -> Vec<MulticastShredCheckService> {
    if !enabled {
        return Vec::new();
    }
    let Some((leader_addr, root_addr)) = addresses_for_cluster(cluster_type) else {
        return Vec::new();
    };
    vec![
        MulticastShredCheckService::new(exit.clone(), leader_receiver_address, leader_addr),
        MulticastShredCheckService::new(exit, root_receiver_address, root_addr),
    ]
}
