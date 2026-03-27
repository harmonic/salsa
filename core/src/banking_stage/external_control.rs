//! Control message handler thread for the external scheduler.
//!
//! Spawned as `solEControl` alongside the external worker threads.
//! Reads `ControlMessage` from the scheduler via a shaq queue and
//! manages the validator's gossip TPU address accordingly.

use {
    agave_scheduler_bindings::{ControlMessage, control_tags},
    solana_gossip::cluster_info::ClusterInfo,
    std::{
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        thread::JoinHandle,
    },
};

/// Spawn the external control handler thread.
///
/// Polls the control queue on every iteration (negligible cost on an
/// empty queue — single cache-line read) for ~1 second latency from
/// relayer crash to TPU restoration.
pub fn spawn(
    exit: Arc<AtomicBool>,
    mut consumer: shaq::Consumer<ControlMessage>,
    cluster_info: Arc<ClusterInfo>,
    configured_tpu: Arc<Mutex<SocketAddr>>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("solEControl".to_string())
        .spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                consumer.sync();
                while let Some(msg) = consumer.try_read() {
                    match msg.tag {
                        control_tags::SET_PROXY_TPU => {
                            if let Some(proxy) =
                                parse_proxy_address(&msg.proxy_tpu_address, msg.proxy_tpu_port)
                            {
                                match cluster_info.set_tpu_quic(proxy) {
                                    Ok(()) => log::info!("Set proxy TPU address to {proxy}"),
                                    Err(err) => {
                                        log::error!("Failed to set proxy TPU: {err}")
                                    }
                                }
                            }
                        }
                        control_tags::RESTORE_ORIGINAL_TPU => {
                            let configured = *configured_tpu.lock().unwrap();
                            match cluster_info.set_tpu_quic(configured) {
                                Ok(()) => {
                                    log::info!("Restored TPU to configured address {configured}")
                                }
                                Err(err) => {
                                    log::error!("Failed to restore TPU: {err}")
                                }
                            }
                        }
                        _ => log::warn!("Unknown control message tag: {}", msg.tag),
                    }
                }
                consumer.finalize();
                std::thread::yield_now();
            }
        })
        .unwrap()
}

/// Parse a proxy address from the wire format (IPv6-mapped, 16-byte array + port).
fn parse_proxy_address(addr: &[u8; 16], port: u16) -> Option<SocketAddr> {
    if port == 0 {
        return None;
    }
    if *addr == [0u8; 16] {
        return None;
    }
    if addr[..10] == [0u8; 10] && addr[10] == 0xff && addr[11] == 0xff {
        let ip = Ipv4Addr::new(addr[12], addr[13], addr[14], addr[15]);
        Some(SocketAddr::new(IpAddr::V4(ip), port))
    } else {
        let ip = Ipv6Addr::from(*addr);
        Some(SocketAddr::new(IpAddr::V6(ip), port))
    }
}
