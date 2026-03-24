//! Multicast root forwarder: forward at the turbine root to a dedicated
//! multicast group.
//!
//! When this validator is the turbine root for a slot, the retransmit stage
//! mirrors the shred to a multicast destination so peers subscribed to the
//! group receive it. The leader broadcasts to its own multicast group
//! (`MULTICAST_SHRED_ADDR_*`); the root forwards to a separate group
//! (`MULTICAST_ROOT_SHRED_ADDR_*`), so the two paths do not duplicate at the
//! receiver.
//!
//! The feature is active whenever `MulticastShredCheckService` has published a
//! receiver address — i.e. the validator's kernel has a host route for the
//! root multicast group. No address published means no forwarding.

use {
    arc_swap::ArcSwap,
    std::{net::SocketAddr, sync::Arc},
};

/// Inputs the retransmit stage needs to drive the multicast-root forwarding
/// decision. `Some(_)` means the feature is enabled; `None` disables it.
pub struct MulticastRootConfig {
    /// Current multicast destination (kept in sync with kernel route state by
    /// `MulticastShredCheckService`). Inner `Option` is `None` when the route
    /// is not present.
    pub receiver_address: Arc<ArcSwap<Option<SocketAddr>>>,
}

/// Returns the multicast address to append to `external_addrs` for this shred,
/// or `None` when no forward is needed.
///
/// Forwarding only happens at the turbine root (`root_distance == 0`) and only
/// while `MulticastShredCheckService` has published a receiver address.
pub fn maybe_external_addr(
    config: Option<&MulticastRootConfig>,
    root_distance: u8,
) -> Option<SocketAddr> {
    if root_distance != 0 {
        return None;
    }
    **config?.receiver_address.load()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::net::{IpAddr, Ipv4Addr},
    };

    fn make_config(addr: Option<SocketAddr>) -> MulticastRootConfig {
        MulticastRootConfig {
            receiver_address: Arc::new(ArcSwap::from_pointee(addr)),
        }
    }

    const MCAST: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(233, 84, 178, 16)), 7733);

    #[test]
    fn maybe_external_addr_emits_at_turbine_root() {
        let cfg = make_config(Some(MCAST));
        assert_eq!(maybe_external_addr(Some(&cfg), 0), Some(MCAST));
    }

    #[test]
    fn maybe_external_addr_skipped_when_not_root() {
        let cfg = make_config(Some(MCAST));
        assert_eq!(maybe_external_addr(Some(&cfg), 1), None);
    }

    #[test]
    fn maybe_external_addr_skipped_when_address_unset() {
        let cfg = make_config(None);
        assert_eq!(maybe_external_addr(Some(&cfg), 0), None);
    }

    #[test]
    fn maybe_external_addr_skipped_when_no_config() {
        assert_eq!(maybe_external_addr(None, 0), None);
    }
}
