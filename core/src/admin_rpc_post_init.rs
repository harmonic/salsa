use {
    crate::{
        banking_stage::BankingControlMsg,
        cluster_slots_service::cluster_slots::ClusterSlots,
        repair::{outstanding_requests::OutstandingRequests, serve_repair::ShredRepairType},
    },
    arc_swap::ArcSwap,
    solana_gossip::{cluster_info::ClusterInfo, node::NodeMultihoming},
    solana_pubkey::Pubkey,
    solana_runtime::{bank_forks::BankForks, snapshot_controller::SnapshotController},
    solana_tls_utils::NotifyKeyUpdate,
    solana_turbine::ShredReceiverAddresses,
    std::{
        collections::{HashMap, HashSet},
        net::{SocketAddr, UdpSocket},
        sync::{Arc, Mutex, RwLock, atomic::AtomicBool},
    },
    tokio::sync::mpsc,
};

/// Key updaters:
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum KeyUpdaterType {
    /// TPU key updater
    Tpu,
    /// TPU forwards key updater
    TpuForwards,
    /// TPU vote key updater
    TpuVote,
    /// Forward key updater
    Forward,
    /// For the RPC service
    RpcService,
    /// BLS all-to-all streamer key updater
    Bls,
    /// BLS all-to-all connection cache key updater
    BlsConnectionCache,
}

/// Responsible for managing the updaters for identity key change
#[derive(Default)]
pub struct KeyUpdaters {
    updaters: HashMap<KeyUpdaterType, Arc<dyn NotifyKeyUpdate + Sync + Send>>,
}

impl KeyUpdaters {
    /// Add a new key updater to the list
    pub fn add(
        &mut self,
        updater_type: KeyUpdaterType,
        updater: Arc<dyn NotifyKeyUpdate + Sync + Send>,
    ) {
        self.updaters.insert(updater_type, updater);
    }

    /// Remove a key updater by its key
    pub fn remove(&mut self, updater_type: &KeyUpdaterType) {
        self.updaters.remove(updater_type);
    }
}

/// Implement the Iterator trait for KeyUpdaters
impl<'a> IntoIterator for &'a KeyUpdaters {
    type Item = (
        &'a KeyUpdaterType,
        &'a Arc<dyn NotifyKeyUpdate + Sync + Send>,
    );
    type IntoIter = std::collections::hash_map::Iter<
        'a,
        KeyUpdaterType,
        Arc<dyn NotifyKeyUpdate + Sync + Send>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.updaters.iter()
    }
}

#[derive(Clone)]
pub struct AdminRpcRequestMetadataPostInit {
    pub cluster_info: Arc<ClusterInfo>,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub vote_account: Pubkey,
    pub repair_whitelist: Arc<RwLock<HashSet<Pubkey>>>,
    pub notifies: Arc<RwLock<KeyUpdaters>>,
    pub repair_socket: Arc<UdpSocket>,
    pub outstanding_repair_requests: Arc<RwLock<OutstandingRequests<ShredRepairType>>>,
    pub cluster_slots: Arc<ClusterSlots>,
    pub node: Option<Arc<NodeMultihoming>>,
    pub banking_control_sender: mpsc::Sender<BankingControlMsg>,
    pub snapshot_controller: Arc<SnapshotController>,
    pub shred_receiver_addresses: Arc<ArcSwap<ShredReceiverAddresses>>,
    pub shred_retransmit_receiver_addresses: Arc<ArcSwap<ShredReceiverAddresses>>,
    /// True when an external scheduler is managing the TPU address.
    pub external_scheduler_active: Arc<AtomicBool>,
    /// Configured TPU address — updated by admin RPC when external scheduler is active,
    /// restored when the scheduler disconnects.
    pub configured_tpu: Arc<Mutex<SocketAddr>>,
}
