//! TPU addresses an external scheduler advertises in gossip for the life of its session.

use {
    log::{error, info},
    solana_gossip::{
        cluster_info::ClusterInfo,
        contact_info::{Error as ContactInfoError, Protocol},
    },
    std::{net::SocketAddr, sync::Arc},
    thiserror::Error,
};

#[derive(Error, Debug)]
pub enum TpuOverrideError {
    #[error("no external scheduler session")]
    NoSession,
    #[error("the node does not publish QUIC TPU addresses")]
    NotPublished,
    #[error("invalid TPU address: {0}")]
    InvalidAddress(#[from] ContactInfoError),
}

pub struct TpuOverride {
    cluster_info: Arc<ClusterInfo>,
    active_session: bool,
    /// The node's local `(tpu, tpu_forwards)` while the override is advertised
    local_addresses: Option<(SocketAddr, SocketAddr)>,
}

impl TpuOverride {
    pub fn new(cluster_info: Arc<ClusterInfo>) -> Self {
        Self {
            cluster_info,
            active_session: false,
            local_addresses: None,
        }
    }

    pub(crate) fn start_session(&mut self) {
        self.active_session = true;
    }

    /// Advertise `tpu` and `tpu_forwards` until [`Self::clear`] or the session ends.
    pub fn set(
        &mut self,
        tpu: SocketAddr,
        tpu_forwards: SocketAddr,
    ) -> Result<(), TpuOverrideError> {
        if !self.active_session {
            return Err(TpuOverrideError::NoSession);
        }
        let mut contact_info = self.cluster_info.my_contact_info();
        let advertised = contact_info
            .tpu(Protocol::QUIC)
            .zip(contact_info.tpu_forwards(Protocol::QUIC))
            .ok_or(TpuOverrideError::NotPublished)?;
        // Check both addresses before advertising either
        contact_info.set_tpu(Protocol::QUIC, tpu)?;
        contact_info.set_tpu_forwards(Protocol::QUIC, tpu_forwards)?;
        // Save the node's local addresses once; a later call would save the previous override
        self.local_addresses.get_or_insert(advertised);
        self.cluster_info.set_tpu_quic(tpu)?;
        self.cluster_info.set_tpu_forwards_quic(tpu_forwards)?;
        info!("Public TPU addresses overridden: tpu={tpu} tpu_forwards={tpu_forwards}");
        Ok(())
    }

    /// Advertise the node's local addresses again.
    pub fn clear(&mut self) {
        let Some((tpu, tpu_forwards)) = self.local_addresses.take() else {
            return;
        };
        match self
            .cluster_info
            .set_tpu_quic(tpu)
            .and_then(|()| self.cluster_info.set_tpu_forwards_quic(tpu_forwards))
        {
            Ok(()) => info!("Public TPU addresses restored: tpu={tpu} tpu_forwards={tpu_forwards}"),
            Err(err) => error!("Failed to restore public TPU addresses: {err}"),
        }
    }

    pub(crate) fn end_session(&mut self) {
        self.active_session = false;
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, solana_gossip::contact_info::ContactInfo, solana_keypair::Keypair,
        solana_net_utils::SocketAddrSpace, solana_signer::Signer,
    };

    fn tpu_override() -> TpuOverride {
        let keypair = Arc::new(Keypair::new());
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), 0);
        TpuOverride::new(Arc::new(ClusterInfo::new(
            contact_info,
            keypair,
            SocketAddrSpace::Unspecified,
        )))
    }

    fn advertised(tpu_override: &TpuOverride) -> (SocketAddr, SocketAddr) {
        let contact_info = tpu_override.cluster_info.my_contact_info();
        (
            contact_info.tpu(Protocol::QUIC).unwrap(),
            contact_info.tpu_forwards(Protocol::QUIC).unwrap(),
        )
    }

    #[test]
    fn test_override_is_scoped_to_session() {
        let mut tpu_override = tpu_override();
        let local = advertised(&tpu_override);
        let remote = (
            "10.0.0.1:8009".parse().unwrap(),
            "10.0.0.1:8010".parse().unwrap(),
        );

        assert!(matches!(
            tpu_override.set(remote.0, remote.1),
            Err(TpuOverrideError::NoSession)
        ));
        assert_eq!(advertised(&tpu_override), local);

        tpu_override.start_session();
        tpu_override.set(remote.0, remote.1).unwrap();
        assert_eq!(advertised(&tpu_override), remote);
        // A second override keeps the node's local addresses
        tpu_override.set(remote.1, remote.0).unwrap();
        tpu_override.end_session();
        assert_eq!(advertised(&tpu_override), local);
        assert!(matches!(
            tpu_override.set(remote.0, remote.1),
            Err(TpuOverrideError::NoSession)
        ));

        tpu_override.start_session();
        tpu_override.set(remote.0, remote.1).unwrap();
        tpu_override.clear();
        assert_eq!(advertised(&tpu_override), local);
        // Nothing to clear
        tpu_override.clear();
        tpu_override.end_session();
        assert_eq!(advertised(&tpu_override), local);
    }

    #[test]
    fn test_rejected_override_changes_nothing() {
        let mut tpu_override = tpu_override();
        let local = advertised(&tpu_override);
        tpu_override.start_session();
        // Gossip rejects port 0
        assert!(matches!(
            tpu_override.set(
                "10.0.0.1:8009".parse().unwrap(),
                "10.0.0.1:0".parse().unwrap()
            ),
            Err(TpuOverrideError::InvalidAddress(_))
        ));
        assert_eq!(advertised(&tpu_override), local);
    }
}
