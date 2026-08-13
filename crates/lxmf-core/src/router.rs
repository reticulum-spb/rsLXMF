//! LXMF Router: message delivery engine and propagation node.
//!
//! Python reference: LXMF/LXMRouter.py. Actor pattern — a single tokio task owns
//! all mutable state.

use crate::now_f64;
use std::collections::HashMap;
use std::fmt;

use tokio::sync::{mpsc, oneshot};

use crate::constants::*;
use crate::message::{LxMessage, MessageCallbacks, MessageError};
use crate::peer::LxmPeer;
use crate::propagation::PropagationStore;
use crate::stamper;
use crate::storage::{
    LxmfStorage, MemoryStorage, StorageError, StoredIdentity, StoredMessageMetadata,
    StoredOutboundMessage, StoredOutboundMetadata, StoredRatchet, StoredStampCost, TransientIdKind,
};
use crate::ticket::{Ticket, TicketStore};
use crate::types::PropagationTransientId;

/// Router configuration.
///
/// Core fields are stable for downstream compatibility; additional Python
/// `LXMRouter.__init__` knobs live in [`RouterConfigExt`] behind the `ext` field.
#[derive(Debug, Clone)]
pub struct RouterConfig {
    pub propagation_enabled: bool,
    pub autopeer: bool,
    pub max_peers: usize,
    pub propagation_limit_kb: usize,
    pub delivery_limit_kb: usize,
    pub sync_limit_kb: usize,
    pub propagation_stamp_cost: u8,
    pub propagation_stamp_flex: u8,
    pub stamp_cost: Option<u8>,
    pub ext: RouterConfigExt,
}

/// Extended router configuration.
///
/// Additional `LXMRouter.__init__` fields; all have sensible defaults.
#[derive(Debug, Clone)]
pub struct RouterConfigExt {
    pub autopeer_maxdepth: usize,
    pub propagation_cost_min: u8,
    pub peering_cost: u8,
    pub max_peering_cost: u8,
    pub processing_outbound: bool,
    /// Maximum outbound messages to process per tick (`None` = unlimited).
    pub processing_limit: Option<usize>,
    pub retain_synced_on_node: bool,
    pub auth_required: bool,
    /// Generate outbound message PoW stamps through the router deferred-stamp queue.
    pub defer_stamp_generation: bool,
    /// Propagation storage cap in bytes (`None` = unlimited).
    pub message_storage_limit: Option<usize>,
    /// Name advertised in propagation announce metadata.
    pub name: Option<String>,
    pub from_static_only: bool,
}

impl Default for RouterConfigExt {
    fn default() -> Self {
        Self {
            autopeer_maxdepth: AUTOPEER_MAXDEPTH,
            propagation_cost_min: PROPAGATION_COST_MIN,
            peering_cost: PEERING_COST,
            max_peering_cost: MAX_PEERING_COST,
            processing_outbound: true,
            processing_limit: None,
            retain_synced_on_node: false,
            auth_required: false,
            defer_stamp_generation: true,
            message_storage_limit: None,
            name: None,
            from_static_only: false,
        }
    }
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            propagation_enabled: false,
            autopeer: AUTOPEER,
            max_peers: MAX_PEERS,
            propagation_limit_kb: PROPAGATION_LIMIT,
            delivery_limit_kb: DELIVERY_LIMIT,
            sync_limit_kb: SYNC_LIMIT,
            propagation_stamp_cost: PROPAGATION_COST,
            propagation_stamp_flex: PROPAGATION_COST_FLEX,
            stamp_cost: None,
            ext: RouterConfigExt::default(),
        }
    }
}

pub struct DeferredStampJob {
    pub message_hash: [u8; 32],
    handle: stamper::DeferredStampHandle,
    rx: oneshot::Receiver<stamper::DeferredStampResult>,
}

#[derive(Debug)]
pub enum SendError {
    MissingOutboundPropagationNode(Box<LxMessage>),
}

impl SendError {
    pub fn message(&self) -> &LxMessage {
        match self {
            Self::MissingOutboundPropagationNode(message) => message,
        }
    }
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingOutboundPropagationNode(_) => f.write_str(
                "attempt to send propagated message with no outbound propagation node configured",
            ),
        }
    }
}

impl std::error::Error for SendError {}

/// Current route metadata for an LXMF Direct delivery destination.
#[derive(Debug, Clone, PartialEq)]
pub struct DirectRouteSnapshot {
    pub destination_hash: [u8; 16],
    pub hops: u8,
    pub interface_name: Option<String>,
    pub learned_at: Option<f64>,
    pub expires_at: Option<f64>,
}

impl DirectRouteSnapshot {
    pub fn new(destination_hash: [u8; 16], hops: u8) -> Self {
        Self {
            destination_hash,
            hops: hops.max(1),
            interface_name: None,
            learned_at: None,
            expires_at: None,
        }
    }
}

/// Reusable Direct/backchannel Link state visible to the router-level policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectReusableLinkState {
    None,
    Pending,
    Active,
    Closed { activated: bool },
}

/// Input snapshot for core Direct-delivery planning.
#[derive(Debug, Clone, PartialEq)]
pub struct DirectDeliveryPlanInput {
    pub identity_known: bool,
    pub route: Option<DirectRouteSnapshot>,
    pub reusable_link: DirectReusableLinkState,
}

/// Core Direct-delivery policy decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectDeliveryPlan {
    UseReusableLink,
    WaitForReusableLink,
    StartNewLink { hops: u8 },
    RequestPath { drop_existing: bool },
    DeferTerminalFailure,
    Fail,
}

/// Apply upstream LXMF Direct delivery attempt/path/link policy to one message.
///
/// This is a policy primitive for embedders that still own transport adapters.
/// It mutates the same attempt/progress fields Python mutates in
/// `LXMRouter.process_outbound`, but leaves actual path requests and Link
/// creation to the caller.
pub fn plan_direct_delivery(
    message: &mut LxMessage,
    input: DirectDeliveryPlanInput,
    now: f64,
) -> DirectDeliveryPlan {
    if message.delivery_attempts > MAX_DELIVERY_ATTEMPTS {
        message.mark_failed();
        return DirectDeliveryPlan::Fail;
    }

    match input.reusable_link {
        DirectReusableLinkState::Active => {
            if message.progress < 0.05 {
                message.progress = 0.05;
            }
            if message.state != MessageState::Sending {
                message.mark_sending();
            }
            return DirectDeliveryPlan::UseReusableLink;
        }
        DirectReusableLinkState::Pending => {
            return DirectDeliveryPlan::WaitForReusableLink;
        }
        DirectReusableLinkState::Closed { .. } => {
            message.next_delivery_attempt = now + PATH_REQUEST_WAIT as f64;
            if message.progress < 0.01 {
                message.progress = 0.01;
            }
            return DirectDeliveryPlan::RequestPath {
                drop_existing: true,
            };
        }
        DirectReusableLinkState::None => {}
    }

    if !input.identity_known {
        message.delivery_attempts += 1;
        message.last_delivery_attempt = now;
        message.next_delivery_attempt = now + PATH_REQUEST_WAIT as f64;
        if message.progress < 0.01 {
            message.progress = 0.01;
        }
        return DirectDeliveryPlan::RequestPath {
            drop_existing: false,
        };
    }

    message.delivery_attempts += 1;
    message.last_delivery_attempt = now;
    message.next_delivery_attempt = now + DELIVERY_RETRY_WAIT as f64;

    if message.delivery_attempts >= MAX_DELIVERY_ATTEMPTS {
        return DirectDeliveryPlan::DeferTerminalFailure;
    }

    if let Some(route) = input.route {
        if message.progress < 0.03 {
            message.progress = 0.03;
        }
        DirectDeliveryPlan::StartNewLink {
            hops: route.hops.max(1),
        }
    } else {
        message.next_delivery_attempt = now + PATH_REQUEST_WAIT as f64;
        if message.progress < 0.01 {
            message.progress = 0.01;
        }
        DirectDeliveryPlan::RequestPath {
            drop_existing: false,
        }
    }
}

/// LXMF router — owns all mutable state under the actor pattern.
pub struct LxmRouter {
    storage: Box<dyn LxmfStorage>,
    storage_authoritative: bool,
    outbound_callbacks: HashMap<[u8; 32], MessageCallbacks>,
    pub config: RouterConfig,
    #[deprecated(note = "use bounded outbound query and command methods")]
    pub pending_outbound: Vec<LxMessage>,
    /// Messages awaiting deferred stamp generation, keyed by message hash.
    #[deprecated(note = "use deferred-stamp query and command methods")]
    pub pending_deferred_stamps: HashMap<[u8; 32], LxMessage>,
    /// Captured at construction (or via [`set_runtime_handle`](Self::set_runtime_handle))
    /// so deferred-stamp PoW can spawn onto the blocking pool even when the
    /// caller is itself on a `spawn_blocking` thread, where
    /// `Handle::try_current()` fails. Without it the tick grinds stamps
    /// inline while holding the manager lock (observed 53 s stall).
    pub runtime_handle: Option<tokio::runtime::Handle>,
    pub active_deferred_stamp: Option<DeferredStampJob>,
    /// Identities allowed for delivery. An empty list means "all allowed".
    pub allowed: Vec<[u8; 16]>,
    pub blocked: Vec<[u8; 16]>,
    pub allowed_control: Vec<[u8; 16]>,
    pub ignored: Vec<[u8; 16]>,
    #[deprecated(note = "use peer summaries and peer command methods")]
    pub peers: HashMap<[u8; 16], LxmPeer>,
    /// Peers that will never be rotated out.
    pub static_peers: Vec<[u8; 16]>,
    #[deprecated(note = "use propagation metadata/payload query methods")]
    pub propagation_store: PropagationStore,
    /// Cached stamp costs keyed by destination hash.
    #[deprecated(note = "use stamp-cost storage-backed methods")]
    pub outbound_stamp_costs: HashMap<[u8; 16], StampCostEntry>,
    #[deprecated(note = "use ticket storage-backed methods")]
    pub ticket_store: TicketStore,
    /// Identity hash → priority level.
    pub prioritized: HashMap<[u8; 16], u8>,
    pub delivery_callback: Option<DeliveryCallback>,
    pub transport_tx: Option<mpsc::Sender<rns_transport::messages::TransportMessage>>,
    /// Throttled peers → expiry timestamp (seconds since UNIX epoch).
    #[deprecated(note = "use throttle command and status methods")]
    pub throttled_peers: HashMap<[u8; 16], f64>,
    pub propagation_start_time: Option<f64>,
    pub processing_count: u64,
    /// Wall-clock gate for `run_jobs_tick`, decoupling jobloop cadence from
    /// the embedder's loop rate.
    last_jobs_tick: f64,
    pub outbound_propagation_node: Option<[u8; 16]>,
    /// Progress in the range 0.0..=1.0.
    pub propagation_transfer_progress: f64,
    pub client_propagation_messages_received: u64,
    pub client_propagation_messages_served: u64,
    pub unpeered_propagation_incoming: u64,
    pub unpeered_propagation_rx_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectOutboundHandling {
    EmitLegacyAction,
    LeavePending,
}

fn decode_message_state(value: u8) -> Result<MessageState, MessageError> {
    match value {
        0x00 => Ok(MessageState::Generating),
        0x01 => Ok(MessageState::Outbound),
        0x02 => Ok(MessageState::Sending),
        0x04 => Ok(MessageState::Sent),
        0x08 => Ok(MessageState::Delivered),
        0xFD => Ok(MessageState::Rejected),
        0xFE => Ok(MessageState::Cancelled),
        0xFF => Ok(MessageState::Failed),
        _ => Err(MessageError::UnpackFailed(format!(
            "invalid message state {value}"
        ))),
    }
}

fn decode_delivery_method(value: u8) -> Result<DeliveryMethod, MessageError> {
    match value {
        0x01 => Ok(DeliveryMethod::Opportunistic),
        0x02 => Ok(DeliveryMethod::Direct),
        0x03 => Ok(DeliveryMethod::Propagated),
        0x05 => Ok(DeliveryMethod::Paper),
        _ => Err(MessageError::UnpackFailed(format!(
            "invalid delivery method {value}"
        ))),
    }
}

fn outbound_summary_from_metadata(metadata: StoredOutboundMetadata) -> Option<OutboundSummary> {
    Some(OutboundSummary {
        message_id: Some(metadata.message_id),
        destination_hash: metadata.destination_hash,
        state: decode_message_state(metadata.state).ok()?,
        method: decode_delivery_method(metadata.delivery_method).ok()?,
        delivery_attempts: metadata.delivery_attempts,
        last_delivery_attempt: metadata.last_delivery_attempt,
        next_delivery_attempt: metadata.next_delivery_attempt,
        progress: metadata.progress,
    })
}

/// Callback invoked when a message is delivered locally.
pub type DeliveryCallback = Box<dyn Fn(&LxMessage) + Send>;

/// Announce-derived data used to create an autopeered propagation peer.
pub struct AutopeerCandidate {
    pub destination_hash: [u8; 16],
    pub timebase: f64,
    pub transfer_limit: Option<f64>,
    pub sync_limit: Option<f64>,
    pub stamp_cost: Option<u8>,
    pub stamp_flexibility: Option<u8>,
    pub peering_cost: Option<u8>,
    pub hops: Option<u8>,
}

/// Cached stamp cost for a destination.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StampCostEntry {
    pub cost: u8,
    pub recorded_at: f64,
}

impl LxmRouter {
    pub fn new(config: RouterConfig) -> Self {
        Self::with_storage_backend(config, Box::new(MemoryStorage::new()))
    }

    /// Construct a router with an actor-owned durable storage backend.
    pub fn with_storage_backend(config: RouterConfig, storage: Box<dyn LxmfStorage>) -> Self {
        Self {
            storage,
            storage_authoritative: false,
            outbound_callbacks: HashMap::new(),
            config,
            pending_outbound: Vec::new(),
            pending_deferred_stamps: HashMap::new(),
            runtime_handle: tokio::runtime::Handle::try_current().ok(),
            active_deferred_stamp: None,
            allowed: Vec::new(),
            blocked: Vec::new(),
            allowed_control: Vec::new(),
            ignored: Vec::new(),
            peers: HashMap::new(),
            static_peers: Vec::new(),
            propagation_store: PropagationStore::new(),
            outbound_stamp_costs: HashMap::new(),
            ticket_store: TicketStore::new(),
            prioritized: HashMap::new(),
            delivery_callback: None,
            transport_tx: None,
            throttled_peers: HashMap::new(),
            propagation_start_time: None,
            processing_count: 0,
            last_jobs_tick: 0.0,
            outbound_propagation_node: None,
            propagation_transfer_progress: 0.0,
            client_propagation_messages_received: 0,
            client_propagation_messages_served: 0,
            unpeered_propagation_incoming: 0,
            unpeered_propagation_rx_bytes: 0,
        }
    }

    pub fn with_shared_storage_backend(
        config: RouterConfig,
        storage: crate::storage::StorageHandle,
    ) -> Self {
        let mut router = Self::with_storage_backend(config, Box::new(storage));
        router.storage_authoritative = true;
        router
    }

    pub fn remember_identity(
        &mut self,
        destination_hash: [u8; 16],
        public_key: [u8; 64],
    ) -> Result<(), StorageError> {
        self.storage.upsert_identity(StoredIdentity {
            destination_hash,
            public_key,
            updated_at: now_f64(),
        })
    }
    pub fn identity(&self, destination_hash: &[u8; 16]) -> Option<[u8; 64]> {
        self.storage
            .identity(destination_hash)
            .ok()
            .flatten()
            .map(|i| i.public_key)
    }
    pub fn identity_cache_seed(&self, limit: usize) -> Vec<([u8; 16], [u8; 64])> {
        self.storage
            .identity_page(limit)
            .unwrap_or_default()
            .into_iter()
            .map(|i| (i.destination_hash, i.public_key))
            .collect()
    }
    pub fn remember_received_ratchet(
        &mut self,
        destination_hash: [u8; 16],
        ratchet_key: [u8; 32],
        received_at: f64,
    ) -> Result<(), StorageError> {
        self.storage.upsert_ratchet(StoredRatchet {
            destination_hash,
            ratchet_key,
            received_at,
        })
    }
    pub fn received_ratchet(&self, h: &[u8; 16]) -> Option<StoredRatchet> {
        self.storage.ratchet(h).ok().flatten()
    }
    pub fn ratchet_cache_seed(&self, cutoff: f64, limit: usize) -> Vec<StoredRatchet> {
        self.storage.ratchet_page(cutoff, limit).unwrap_or_default()
    }
    pub fn cull_received_ratchets_before(&mut self, cutoff: f64) -> usize {
        self.storage
            .cull_ratchets_before(cutoff)
            .unwrap_or_default()
    }
    pub fn put_state_blob(&mut self, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.storage.put_state_blob(key, value)
    }
    pub fn put_state_blobs(&mut self, entries: &[(&str, &[u8])]) -> Result<(), StorageError> {
        self.storage.put_state_blobs(entries)
    }
    pub fn state_blob(&self, key: &str) -> Option<Vec<u8>> {
        self.storage.state_blob(key).ok().flatten()
    }
    pub fn store_inbound_message(
        &mut self,
        id: [u8; 32],
        encoded: &[u8],
    ) -> Result<(), StorageError> {
        self.storage.insert_inbound_message(id, now_f64(), encoded)
    }
    pub fn contains_inbound_message(&self, id: &[u8; 32]) -> bool {
        self.storage.contains_inbound_message(id).unwrap_or(false)
    }
    pub fn load_persisted_peers(&mut self) -> usize {
        let limit = self.config.max_peers;
        let peers = self.storage.peer_page(limit).unwrap_or_default();
        for (hash, encoded) in peers {
            if let Some(peer) = LxmPeer::from_bytes_with_handled(&encoded)
                && peer.destination_hash == hash
            {
                self.peers.insert(hash, peer);
            }
        }
        self.peers.len()
    }
    pub fn checkpoint_peers(&mut self) -> Result<(), StorageError> {
        let peers = self
            .peers
            .iter()
            .map(|(hash, peer)| (*hash, peer.to_bytes_with_handled()))
            .collect::<Vec<_>>();
        self.storage.replace_peers(&peers)
    }

    fn persist_outbound_message(&mut self, message: &LxMessage, deferred: bool) -> bool {
        let Some(message_id) = message.message_id.or(message.hash) else {
            tracing::error!("cannot persist outbound message without message ID");
            return false;
        };
        let encoded_message = match message.encode_outbound_storage() {
            Ok(encoded) => encoded,
            Err(error) => {
                tracing::error!(%error, "failed to encode outbound message");
                return false;
            }
        };
        let stored = StoredOutboundMessage {
            metadata: StoredOutboundMetadata {
                message_id,
                destination_hash: message.destination_hash,
                state: message.state as u8,
                delivery_method: message.method as u8,
                deferred,
                next_delivery_attempt: message.next_delivery_attempt,
                last_delivery_attempt: message.last_delivery_attempt,
                delivery_attempts: message.delivery_attempts,
                created_at: message.timestamp,
                progress: message.progress,
            },
            encoded_message,
        };
        if let Err(error) = self.storage.upsert_outbound_message(&stored) {
            tracing::error!(%error, "failed to persist outbound message");
            return false;
        }
        self.outbound_callbacks
            .insert(message_id, message.callbacks.clone());
        true
    }

    fn restore_outbound_message(
        &self,
        stored: StoredOutboundMessage,
    ) -> Result<LxMessage, MessageError> {
        let metadata = stored.metadata;
        let mut message = LxMessage::decode_outbound_storage(&stored.encoded_message)?;
        message.hash = Some(metadata.message_id);
        message.message_id = Some(metadata.message_id);
        message.transient_id = Some(metadata.message_id);
        message.state = decode_message_state(metadata.state)?;
        message.method = decode_delivery_method(metadata.delivery_method)?;
        message.next_delivery_attempt = metadata.next_delivery_attempt;
        message.last_delivery_attempt = metadata.last_delivery_attempt;
        message.delivery_attempts = metadata.delivery_attempts;
        message.timestamp = metadata.created_at;
        message.progress = metadata.progress;
        if let Some(callbacks) = self.outbound_callbacks.get(&metadata.message_id) {
            message.callbacks = callbacks.clone();
        }
        Ok(message)
    }

    fn hydrate_ready_outbound(&mut self) {
        if !self.storage_authoritative || !self.pending_outbound.is_empty() {
            return;
        }
        let Ok(ready) = self.storage.outbound_ready(now_f64(), false, 1) else {
            return;
        };
        let Some(metadata) = ready.first() else {
            return;
        };
        match self.storage.outbound_message(&metadata.message_id) {
            Ok(Some(stored)) => match self.restore_outbound_message(stored) {
                Ok(message) => self.pending_outbound.push(message),
                Err(error) => tracing::error!(%error, "failed to restore outbound message"),
            },
            Ok(None) => {}
            Err(error) => tracing::error!(%error, "failed to load outbound message"),
        }
    }

    fn hydrate_deferred_stamp(&mut self) {
        if !self.storage_authoritative
            || !self.pending_deferred_stamps.is_empty()
            || self.active_deferred_stamp.is_some()
        {
            return;
        }
        let Ok(ready) = self.storage.outbound_ready(now_f64(), true, 1) else {
            return;
        };
        let Some(metadata) = ready.first() else {
            return;
        };
        if let Ok(Some(stored)) = self.storage.outbound_message(&metadata.message_id) {
            match self.restore_outbound_message(stored) {
                Ok(message) => {
                    self.pending_deferred_stamps
                        .insert(metadata.message_id, message);
                }
                Err(error) => tracing::error!(%error, "failed to restore deferred stamp message"),
            }
        }
    }

    pub fn is_locally_delivered(
        &mut self,
        transient_id: &PropagationTransientId,
    ) -> Result<bool, StorageError> {
        self.storage
            .contains_transient_id(TransientIdKind::LocallyDelivered, transient_id)
    }

    pub fn is_locally_processed(
        &mut self,
        transient_id: &PropagationTransientId,
    ) -> Result<bool, StorageError> {
        self.storage
            .contains_transient_id(TransientIdKind::LocallyProcessed, transient_id)
    }

    pub fn mark_locally_delivered(
        &mut self,
        transient_id: PropagationTransientId,
    ) -> Result<(), StorageError> {
        self.storage.upsert_transient_id(
            TransientIdKind::LocallyDelivered,
            transient_id,
            now_f64() as i64,
        )
    }

    pub fn mark_locally_processed(
        &mut self,
        transient_id: PropagationTransientId,
    ) -> Result<(), StorageError> {
        self.storage.upsert_transient_id(
            TransientIdKind::LocallyProcessed,
            transient_id,
            now_f64() as i64,
        )
    }

    pub fn cull_transient_ids(&mut self, now: i64) -> Result<usize, StorageError> {
        let retention = i64::try_from(MESSAGE_EXPIRY.saturating_mul(6)).unwrap_or(i64::MAX);
        self.storage
            .cull_transient_ids_before(now.saturating_sub(retention))
    }

    pub fn set_transport(&mut self, tx: mpsc::Sender<rns_transport::messages::TransportMessage>) {
        self.transport_tx = Some(tx);
    }

    /// Return an owned snapshot of identities allowed to use the control API.
    pub fn control_allowed_identities(&self, limit: usize) -> Vec<[u8; 16]> {
        self.allowed_control.iter().copied().take(limit).collect()
    }

    pub fn control_allowed_count(&self) -> usize {
        self.allowed_control.len()
    }

    /// Return an owned snapshot of configured peers.
    pub fn peer_hashes(&self, limit: usize) -> Vec<[u8; 16]> {
        self.peers.keys().copied().take(limit).collect()
    }

    pub fn has_peer(&self, destination_hash: &[u8; 16]) -> bool {
        self.peers.contains_key(destination_hash)
    }

    /// Add a peer and mark it static. Returns whether either set changed.
    pub fn add_static_peer(&mut self, destination_hash: [u8; 16]) -> bool {
        let peer_was_missing = !self.peers.contains_key(&destination_hash);
        let peer = self
            .peers
            .entry(destination_hash)
            .or_insert_with(|| LxmPeer::new(destination_hash));
        peer.is_static = true;
        let mut changed = peer_was_missing;
        if !self.static_peers.contains(&destination_hash) {
            self.static_peers.push(destination_hash);
            changed = true;
        }
        changed
    }

    /// Mark a known peer ready for an immediate sync attempt.
    pub fn request_peer_sync(&mut self, destination_hash: &[u8; 16]) -> bool {
        let Some(peer) = self.peers.get_mut(destination_hash) else {
            return false;
        };
        peer.next_sync_attempt = 0.0;
        peer.alive = true;
        true
    }

    pub fn outbound_propagation_node(&self) -> Option<[u8; 16]> {
        self.outbound_propagation_node
    }

    /// Return a bounded, payload-free snapshot of queued outbound messages.
    pub fn outbound_summaries(&self, limit: usize) -> Vec<OutboundSummary> {
        if self.storage_authoritative {
            return self
                .storage
                .outbound_metadata_page(false, limit)
                .unwrap_or_default()
                .into_iter()
                .filter_map(outbound_summary_from_metadata)
                .collect();
        }
        self.pending_outbound
            .iter()
            .take(limit)
            .map(|message| OutboundSummary {
                message_id: message.message_id.or(message.hash),
                destination_hash: message.destination_hash,
                state: message.state,
                method: message.method,
                delivery_attempts: message.delivery_attempts,
                last_delivery_attempt: message.last_delivery_attempt,
                next_delivery_attempt: message.next_delivery_attempt,
                progress: message.progress,
            })
            .collect()
    }

    /// Point lookup used to decide whether a link-delivery result is still
    /// owned by this router without exposing the compatibility queue.
    pub fn has_pending_outbound(&self, message_id: &[u8; 32]) -> bool {
        self.pending_outbound.iter().any(|message| {
            message.message_id == Some(*message_id) || message.hash == Some(*message_id)
        })
    }

    /// Query one deferred-stamp message without exposing the backing map.
    pub fn deferred_stamp_summary(&self, message_id: &[u8; 32]) -> Option<OutboundSummary> {
        if self.storage_authoritative {
            return self
                .storage
                .outbound_message(message_id)
                .ok()
                .flatten()
                .filter(|message| message.metadata.deferred)
                .and_then(|message| outbound_summary_from_metadata(message.metadata));
        }
        self.pending_deferred_stamps
            .get(message_id)
            .map(|message| OutboundSummary {
                message_id: message.message_id.or(message.hash),
                destination_hash: message.destination_hash,
                state: message.state,
                method: message.method,
                delivery_attempts: message.delivery_attempts,
                last_delivery_attempt: message.last_delivery_attempt,
                next_delivery_attempt: message.next_delivery_attempt,
                progress: message.progress,
            })
    }

    /// Look up propagation metadata without reading a message payload.
    pub fn propagation_metadata(
        &self,
        transient_id: &PropagationTransientId,
    ) -> Result<Option<StoredMessageMetadata>, StorageError> {
        self.storage.message_metadata(transient_id)
    }

    /// Return a deterministic, bounded page of propagation metadata.
    ///
    /// `after` is an exclusive transient-ID cursor from the previous page.
    pub fn propagation_metadata_page(
        &self,
        after: Option<&PropagationTransientId>,
        limit: usize,
    ) -> Result<Vec<StoredMessageMetadata>, StorageError> {
        self.storage.message_metadata_page(after, limit)
    }

    /// Start propagation runtime accounting if it has not already started.
    pub fn ensure_propagation_started(&mut self) {
        if self.propagation_start_time.is_none() {
            self.propagation_start_time = Some(now_f64());
        }
    }

    pub fn extend_allowed<I>(&mut self, identities: I)
    where
        I: IntoIterator<Item = [u8; 16]>,
    {
        for identity in identities {
            self.allow(identity);
        }
    }

    pub fn extend_ignored<I>(&mut self, destinations: I)
    where
        I: IntoIterator<Item = [u8; 16]>,
    {
        for destination in destinations {
            self.ignore_destination(destination);
        }
    }

    pub fn has_transport(&self) -> bool {
        self.transport_tx.is_some()
    }

    /// Queue a message for outbound delivery.
    ///
    /// Opportunistic messages that exceed the single-packet ceiling are
    /// transparently downgraded to Direct delivery.
    #[tracing::instrument(
        level = "debug",
        name = "router.send",
        skip_all,
        fields(
            destination_hash = %hex::encode(&message.destination_hash[..8]),
            method = ?message.method,
            content_len = message.content.len(),
        ),
    )]
    pub fn send(&mut self, message: LxMessage) {
        let _ = self.try_send(message);
    }

    /// Queue a message for outbound delivery and report immediate routing errors.
    ///
    /// Mirrors Python LXMF 0.9.8's explicit `IOError` when a caller attempts
    /// `PROPAGATED` delivery without configuring an outbound propagation node.
    /// The legacy [`send`](Self::send) wrapper preserves older Rust call-sites
    /// while still marking the message failed and firing its callback.
    pub fn try_send(&mut self, mut message: LxMessage) -> Result<(), SendError> {
        if message.method == DeliveryMethod::Propagated && self.outbound_propagation_node.is_none()
        {
            message.progress = 0.0;
            if message.state != MessageState::Rejected {
                message.mark_failed();
            } else {
                message.notify_failed();
            }
            return Err(SendError::MissingOutboundPropagationNode(Box::new(message)));
        }

        let now = now_f64();
        if message.outbound_ticket.is_none() {
            let ticket = if self.storage_authoritative {
                self.storage
                    .valid_ticket(&message.destination_hash, now)
                    .ok()
                    .flatten()
            } else {
                self.ticket_store
                    .find(&message.destination_hash, now)
                    .cloned()
            };
            if let Some(ticket) = ticket {
                message.outbound_ticket = Some(ticket.token);
            }
        }

        if message.stamp.is_none() && message.stamp_cost.is_none() {
            message.stamp_cost = self
                .get_stamp_cost(&message.destination_hash)
                .filter(|cost| *cost > 0);
        }

        if message.stamp.is_none()
            && message.outbound_ticket.is_some()
            && (message.message_id.is_some() || message.compute_hash().is_ok())
        {
            message.get_stamp();
        }

        if message.stamp.is_none()
            && message.stamp_cost.unwrap_or(0) > 0
            && (message.message_id.is_some() || message.compute_hash().is_ok())
        {
            if self.config.ext.defer_stamp_generation {
                if let Some(message_hash) = message.message_id.or(message.hash) {
                    message.state = MessageState::Outbound;
                    if self.storage_authoritative {
                        if !self.persist_outbound_message(&message, true) {
                            message.mark_failed();
                        }
                    } else {
                        self.pending_deferred_stamps.insert(message_hash, message);
                    }
                    return Ok(());
                }
            } else {
                message.get_stamp();
            }
        }

        if message.method == DeliveryMethod::Opportunistic
            && let Ok(packed) = message.pack_payload()
        {
            let content_size = packed
                .len()
                .saturating_sub(TIMESTAMP_SIZE + STRUCT_OVERHEAD);
            // Approximates ENCRYPTED_PACKET_MAX_CONTENT for default RNS parameters.
            let max_content = 295;
            if content_size > max_content {
                message.method = DeliveryMethod::Direct;
            }
        }

        message.state = MessageState::Outbound;
        if self.storage_authoritative {
            if !self.persist_outbound_message(&message, false) {
                message.mark_failed();
            }
        } else {
            self.pending_outbound.push(message);
        }
        Ok(())
    }

    /// Queue `message` for off-thread stamp generation; it re-enters
    /// `pending_outbound` with the stamp attached once the worker finishes.
    /// Returns the message back when it carries no id to key the job on.
    pub fn defer_stamp(&mut self, mut message: LxMessage) -> Option<LxMessage> {
        let id = message.message_id.or(message.hash).or_else(|| {
            message.compute_hash().ok();
            message.hash
        });
        let Some(message_hash) = id else {
            return Some(message);
        };
        message.state = MessageState::Outbound;
        if self.storage_authoritative {
            if !self.persist_outbound_message(&message, true) {
                return Some(message);
            }
        } else {
            self.pending_deferred_stamps.insert(message_hash, message);
        }
        None
    }

    /// Process deferred outbound message stamp generation.
    ///
    /// Python reference: `LXMRouter.process_deferred_stamps` — LXMRouter.py:2407-2498.
    pub fn process_deferred_stamps(&mut self) {
        self.hydrate_ready_outbound();
        self.hydrate_deferred_stamp();
        self.poll_active_deferred_stamp();
        if self.active_deferred_stamp.is_some() {
            return;
        }

        let Some((&message_hash, message)) = self.pending_deferred_stamps.iter().next() else {
            return;
        };
        let cost = message.stamp_cost.unwrap_or(0);
        if cost == 0 {
            if let Some(mut message) = self.pending_deferred_stamps.remove(&message_hash) {
                message.get_stamp();
                if self.storage_authoritative {
                    self.persist_outbound_message(&message, false);
                } else {
                    self.pending_outbound.push(message);
                }
            }
            return;
        }

        let runtime = tokio::runtime::Handle::try_current()
            .ok()
            .or_else(|| self.runtime_handle.clone());
        if let Some(runtime) = runtime {
            let (handle, rx) = stamper::spawn_deferred_stamp_on(
                &runtime,
                message_hash,
                cost,
                STAMP_WORKBLOCK_EXPAND_ROUNDS,
            );
            self.active_deferred_stamp = Some(DeferredStampJob {
                message_hash,
                handle,
                rx,
            });
        } else if let Some(mut message) = self.pending_deferred_stamps.remove(&message_hash) {
            tracing::warn!(
                cost,
                "no tokio runtime available; generating stamp inline (sync embedder only)"
            );
            match stamper::generate_stamp(&message_hash, cost, STAMP_WORKBLOCK_EXPAND_ROUNDS) {
                Some((stamp, value)) => {
                    message.stamp = Some(stamp.to_vec());
                    message.stamp_value = Some(value as u16);
                    if self.storage_authoritative {
                        self.persist_outbound_message(&message, false);
                    } else {
                        self.pending_outbound.push(message);
                    }
                }
                None => {
                    message.mark_failed();
                    if self.storage_authoritative {
                        let _ = self.storage.remove_outbound_message(&message_hash);
                        self.outbound_callbacks.remove(&message_hash);
                    }
                }
            }
        }
    }

    fn poll_active_deferred_stamp(&mut self) {
        let Some(mut job) = self.active_deferred_stamp.take() else {
            return;
        };

        match job.rx.try_recv() {
            Ok(stamper::DeferredStampResult::Success { stamp, value }) => {
                if let Some(mut message) = self.pending_deferred_stamps.remove(&job.message_hash) {
                    message.stamp = Some(stamp.to_vec());
                    message.stamp_value = Some(value as u16);
                    if self.storage_authoritative {
                        self.persist_outbound_message(&message, false);
                    } else {
                        self.pending_outbound.push(message);
                    }
                }
            }
            Ok(stamper::DeferredStampResult::Cancelled) => {
                if let Some(mut message) = self.pending_deferred_stamps.remove(&job.message_hash) {
                    message.cancel();
                    if self.storage_authoritative {
                        let _ = self.storage.remove_outbound_message(&job.message_hash);
                        self.outbound_callbacks.remove(&job.message_hash);
                    }
                }
            }
            Err(oneshot::error::TryRecvError::Empty) => {
                self.active_deferred_stamp = Some(job);
            }
            Err(oneshot::error::TryRecvError::Closed) => {
                if let Some(mut message) = self.pending_deferred_stamps.remove(&job.message_hash) {
                    message.mark_failed();
                    if self.storage_authoritative {
                        let _ = self.storage.remove_outbound_message(&job.message_hash);
                        self.outbound_callbacks.remove(&job.message_hash);
                    }
                }
            }
        }
    }

    pub fn allow(&mut self, identity_hash: [u8; 16]) {
        if !self.allowed.contains(&identity_hash) {
            self.allowed.push(identity_hash);
        }
    }

    pub fn disallow(&mut self, identity_hash: &[u8; 16]) {
        self.allowed.retain(|h| h != identity_hash);
    }

    pub fn allow_control(&mut self, identity_hash: [u8; 16]) {
        if !self.allowed_control.contains(&identity_hash) {
            self.allowed_control.push(identity_hash);
        }
    }

    pub fn disallow_control(&mut self, identity_hash: &[u8; 16]) {
        self.allowed_control.retain(|h| h != identity_hash);
    }

    pub fn ignore_destination(&mut self, dest_hash: [u8; 16]) {
        if !self.ignored.contains(&dest_hash) {
            self.ignored.push(dest_hash);
        }
        self.propagation_store.ignore_destination(dest_hash);
    }

    pub fn unignore_destination(&mut self, dest_hash: &[u8; 16]) {
        self.ignored.retain(|h| h != dest_hash);
        self.propagation_store.unignore_destination(dest_hash);
    }

    pub fn prioritise(&mut self, identity_hash: [u8; 16], level: u8) {
        self.prioritized.insert(identity_hash, level);
        self.propagation_store.prioritise_destination(identity_hash);
    }

    pub fn unprioritise(&mut self, identity_hash: &[u8; 16]) {
        self.prioritized.remove(identity_hash);
        self.propagation_store
            .unprioritise_destination(identity_hash);
    }

    pub fn block(&mut self, identity_hash: [u8; 16]) {
        if !self.blocked.contains(&identity_hash) {
            self.blocked.push(identity_hash);
        }
    }

    pub fn unblock(&mut self, identity_hash: &[u8; 16]) {
        self.blocked.retain(|h| h != identity_hash);
    }

    /// An empty allow-list means "everyone not blocked is allowed".
    pub fn is_allowed(&self, identity_hash: &[u8; 16]) -> bool {
        if !self.blocked.contains(identity_hash) {
            self.allowed.is_empty() || self.allowed.contains(identity_hash)
        } else {
            false
        }
    }

    pub fn is_control_allowed(&self, identity_hash: &[u8; 16]) -> bool {
        self.allowed_control.contains(identity_hash)
    }

    /// Whether delivery requires an entry in the allow-list.
    ///
    /// Python reference: `LXMRouter.requires_authentication` — LXMRouter.py:415-417.
    pub fn requires_authentication(&self) -> bool {
        self.config.ext.auth_required
    }

    /// Toggle whether delivery requires an entry in the allow-list.
    ///
    /// Python reference: `LXMRouter.set_authentication` — LXMRouter.py:409-413.
    pub fn set_authentication(&mut self, required: bool) {
        self.config.ext.auth_required = required;
    }

    /// Whether the node keeps synchronized messages in its propagation store.
    pub fn retain_node_lxms(&self) -> bool {
        self.config.ext.retain_synced_on_node
    }

    /// Toggle whether the node keeps synchronized messages in its propagation store.
    ///
    /// Python reference: `LXMRouter.set_retain_node_lxms` — LXMRouter.py:419-420.
    pub fn set_retain_node_lxms(&mut self, retain: bool) {
        self.config.ext.retain_synced_on_node = retain;
    }

    /// Propagation storage cap in bytes (`None` = unlimited).
    pub fn message_storage_limit(&self) -> Option<usize> {
        self.config.ext.message_storage_limit
    }

    /// Set the propagation storage cap in bytes (`None` = unlimited).
    ///
    /// Python reference: `LXMRouter.set_message_storage_limit` — LXMRouter.py:423-424.
    pub fn set_message_storage_limit(&mut self, limit: Option<usize>) {
        self.config.ext.message_storage_limit = limit;
    }

    /// Current on-disk-equivalent size of the propagation store, in bytes.
    ///
    /// Python reference: `LXMRouter.message_storage_size` — LXMRouter.py:437-441.
    pub fn message_storage_size(&self) -> usize {
        self.propagation_store.total_size()
    }

    /// Generate a fresh random ticket for `destination_hash` and add it to the ticket store.
    ///
    /// The returned token can be shared with a peer that should bypass stamp PoW when sending
    /// to this router. Default expiry is [`TICKET_EXPIRY`] seconds.
    ///
    /// Python reference: `LXMRouter.generate_ticket` — LXMRouter.py:1094-1108.
    pub fn generate_ticket(
        &mut self,
        destination_hash: [u8; 16],
        expiry_secs: Option<u64>,
    ) -> [u8; 16] {
        use rand::RngCore;
        let mut token = [0u8; TICKET_LENGTH];
        rand::thread_rng().fill_bytes(&mut token);
        let expires = now_f64() + expiry_secs.unwrap_or(TICKET_EXPIRY) as f64;
        let ticket = Ticket::new(token, destination_hash, expires);
        if self.storage_authoritative {
            let _ = self.storage.upsert_ticket(&ticket);
        } else {
            self.ticket_store.add(ticket);
        }
        token
    }

    /// Record an externally-provided outbound ticket so the router can use it when sending.
    ///
    /// Python reference: `LXMRouter.remember_ticket` — LXMRouter.py:1110-1113.
    pub fn remember_ticket(&mut self, destination_hash: [u8; 16], token: [u8; 16], expires: f64) {
        let ticket = Ticket::new(token, destination_hash, expires);
        if self.storage_authoritative {
            let _ = self.storage.upsert_ticket(&ticket);
        } else {
            self.ticket_store.add(ticket);
        }
    }

    pub fn remove_tickets(&mut self, destination_hash: &[u8; 16]) -> usize {
        if self.storage_authoritative {
            self.storage
                .remove_tickets(destination_hash)
                .unwrap_or_default()
        } else {
            self.ticket_store.remove_destination(destination_hash)
        }
    }

    /// Returns the token of the first valid ticket for `destination_hash`.
    ///
    /// Python reference: `LXMRouter.get_outbound_ticket` — LXMRouter.py:1058-1064.
    pub fn get_outbound_ticket(&self, destination_hash: &[u8; 16]) -> Option<[u8; 16]> {
        let now = now_f64();
        if self.storage_authoritative {
            self.storage
                .valid_ticket(destination_hash, now)
                .ok()
                .flatten()
                .map(|ticket| ticket.token)
        } else {
            self.ticket_store
                .find(destination_hash, now)
                .map(|ticket| ticket.token)
        }
    }

    /// Returns the expiry (Unix epoch seconds) of the valid ticket for `destination_hash`.
    ///
    /// Python reference: `LXMRouter.get_outbound_ticket_expiry` — LXMRouter.py:1125-1131.
    pub fn get_outbound_ticket_expiry(&self, destination_hash: &[u8; 16]) -> Option<f64> {
        let now = now_f64();
        if self.storage_authoritative {
            self.storage
                .valid_ticket(destination_hash, now)
                .ok()
                .flatten()
                .map(|ticket| ticket.expires)
        } else {
            self.ticket_store
                .find(destination_hash, now)
                .map(|ticket| ticket.expires)
        }
    }

    /// Snapshot of all stored tickets (including expired / used entries).
    ///
    /// Python reference: `LXMRouter.get_inbound_tickets` — LXMRouter.py:1133-1136.
    pub fn get_inbound_tickets(&self) -> &[Ticket] {
        self.ticket_store.all()
    }

    /// Cancel an outbound message before it is sent.
    ///
    /// Removes the message from `pending_outbound` (or `pending_deferred_stamps`) if it is still
    /// in a cancellable state. Returns `true` if the message was found and cancelled.
    ///
    /// Python reference: `LXMRouter.cancel_outbound` — LXMRouter.py:474-487.
    pub fn cancel_outbound(&mut self, message_hash: &[u8; 32]) -> bool {
        if let Some(pos) = self
            .pending_outbound
            .iter()
            .position(|m| m.hash.as_ref() == Some(message_hash))
        {
            let msg = &mut self.pending_outbound[pos];
            msg.cancel();
            self.pending_outbound.remove(pos);
            if self.storage_authoritative {
                let _ = self.storage.remove_outbound_message(message_hash);
                self.outbound_callbacks.remove(message_hash);
            }
            return true;
        }

        if let Some(mut msg) = self.pending_deferred_stamps.remove(message_hash) {
            msg.cancel();
            if self
                .active_deferred_stamp
                .as_ref()
                .is_some_and(|job| job.message_hash == *message_hash)
                && let Some(job) = self.active_deferred_stamp.take()
            {
                job.handle.cancel();
            }
            if self.storage_authoritative {
                let _ = self.storage.remove_outbound_message(message_hash);
                self.outbound_callbacks.remove(message_hash);
            }
            return true;
        }

        if self.storage_authoritative
            && let Ok(Some(stored)) = self.storage.outbound_message(message_hash)
            && let Ok(mut message) = self.restore_outbound_message(stored)
        {
            message.cancel();
            let removed = self
                .storage
                .remove_outbound_message(message_hash)
                .unwrap_or(false);
            self.outbound_callbacks.remove(message_hash);
            return removed;
        }

        false
    }

    /// Mark a pending outbound message delivered and remove it from the
    /// outbound queue.
    pub fn mark_outbound_delivered(&mut self, message_hash: &[u8; 32]) -> bool {
        let Some(pos) = self
            .pending_outbound
            .iter()
            .position(|m| m.hash.as_ref() == Some(message_hash))
        else {
            if self.storage_authoritative
                && let Ok(Some(stored)) = self.storage.outbound_message(message_hash)
                && let Ok(mut message) = self.restore_outbound_message(stored)
            {
                message.mark_delivered();
                let removed = self
                    .storage
                    .remove_outbound_message(message_hash)
                    .unwrap_or(false);
                self.outbound_callbacks.remove(message_hash);
                return removed;
            }
            return false;
        };

        let mut msg = self.pending_outbound.remove(pos);
        msg.mark_delivered();
        if self.storage_authoritative {
            let _ = self.storage.remove_outbound_message(message_hash);
            self.outbound_callbacks.remove(message_hash);
        }
        true
    }

    /// Mark a pending outbound message failed and remove it from the outbound
    /// queue.
    pub fn mark_outbound_failed(&mut self, message_hash: &[u8; 32]) -> bool {
        let Some(pos) = self
            .pending_outbound
            .iter()
            .position(|m| m.hash.as_ref() == Some(message_hash))
        else {
            if self.storage_authoritative
                && let Ok(Some(stored)) = self.storage.outbound_message(message_hash)
                && let Ok(mut message) = self.restore_outbound_message(stored)
            {
                message.mark_failed();
                let removed = self
                    .storage
                    .remove_outbound_message(message_hash)
                    .unwrap_or(false);
                self.outbound_callbacks.remove(message_hash);
                return removed;
            }
            return false;
        };

        let mut msg = self.pending_outbound.remove(pos);
        msg.mark_failed();
        if self.storage_authoritative {
            let _ = self.storage.remove_outbound_message(message_hash);
            self.outbound_callbacks.remove(message_hash);
        }
        true
    }

    /// Mark a pending outbound message rejected and remove it from the
    /// outbound queue.
    pub fn mark_outbound_rejected(&mut self, message_hash: &[u8; 32]) -> bool {
        let Some(pos) = self
            .pending_outbound
            .iter()
            .position(|m| m.hash.as_ref() == Some(message_hash))
        else {
            if self.storage_authoritative
                && let Ok(Some(stored)) = self.storage.outbound_message(message_hash)
                && let Ok(mut message) = self.restore_outbound_message(stored)
            {
                message.mark_rejected();
                let removed = self
                    .storage
                    .remove_outbound_message(message_hash)
                    .unwrap_or(false);
                self.outbound_callbacks.remove(message_hash);
                return removed;
            }
            return false;
        };

        let mut msg = self.pending_outbound.remove(pos);
        msg.mark_rejected();
        if self.storage_authoritative {
            let _ = self.storage.remove_outbound_message(message_hash);
            self.outbound_callbacks.remove(message_hash);
        }
        true
    }

    /// Re-arm a pending outbound message after a path request without adding a
    /// duplicate queue entry.
    pub fn defer_outbound_for_path_request(&mut self, message_hash: &[u8; 32], now: f64) -> bool {
        let Some(msg) = self
            .pending_outbound
            .iter_mut()
            .find(|m| m.hash.as_ref() == Some(message_hash))
        else {
            if self.storage_authoritative
                && let Ok(Some(mut stored)) = self.storage.outbound_message(message_hash)
            {
                stored.metadata.next_delivery_attempt = now + PATH_REQUEST_WAIT as f64;
                stored.metadata.progress = stored.metadata.progress.max(0.01);
                return self
                    .storage
                    .update_outbound_delivery(&stored.metadata)
                    .unwrap_or(false);
            }
            return false;
        };

        msg.next_delivery_attempt = now + PATH_REQUEST_WAIT as f64;
        if msg.progress < 0.01 {
            msg.progress = 0.01;
        }
        if self.storage_authoritative {
            let stored = msg.clone();
            self.persist_outbound_message(&stored, false);
        }
        true
    }

    /// Get the outbound-delivery progress (0.0..=1.0) for a pending message.
    ///
    /// Python reference: `LXMRouter.get_outbound_progress` — LXMRouter.py:489-495.
    pub fn get_outbound_progress(&self, message_hash: &[u8; 32]) -> Option<f64> {
        let cached = self
            .pending_outbound
            .iter()
            .chain(self.pending_deferred_stamps.values())
            .find(|m| m.hash.as_ref() == Some(message_hash))
            .map(|m| m.progress);
        cached.or_else(|| {
            self.storage_authoritative
                .then(|| self.storage.outbound_message(message_hash).ok().flatten())
                .flatten()
                .map(|stored| stored.metadata.progress)
        })
    }

    /// Get the cached required stamp cost for a destination (delivery).
    ///
    /// Returns `None` if no announce has advertised a cost for this destination.
    ///
    /// Python reference: `LXMRouter.get_outbound_lxm_stamp_cost` — LXMRouter.py:1138-1147.
    pub fn get_outbound_lxm_stamp_cost(&self, destination_hash: &[u8; 16]) -> Option<u8> {
        self.outbound_stamp_costs
            .get(destination_hash)
            .map(|e| e.cost)
    }

    /// Get the propagation stamp cost for a message queued for propagation-node delivery.
    ///
    /// Returns the per-message `stamp_cost` recorded on the pending message when it was enqueued.
    ///
    /// Python reference: `LXMRouter.get_outbound_lxm_propagation_stamp_cost` — LXMRouter.py:1149-1156.
    pub fn get_outbound_lxm_propagation_stamp_cost(&self, message_hash: &[u8; 32]) -> Option<u8> {
        self.pending_outbound
            .iter()
            .chain(self.pending_deferred_stamps.values())
            .find(|m| m.hash.as_ref() == Some(message_hash))
            .and_then(|m| m.stamp_cost)
    }

    /// Ingest an encrypted paper (`lxm://...`) URI and invoke the delivery callback as if the
    /// message had arrived via the network.
    ///
    /// Python reference: `LXMRouter.ingest_lxm_uri` — LXMRouter.py:2370-2385.
    pub fn ingest_lxm_uri<F>(
        &self,
        uri: &str,
        decrypt_fn: F,
    ) -> Result<LxMessage, crate::message::MessageError>
    where
        F: FnOnce(&[u8]) -> Result<Vec<u8>, crate::message::MessageError>,
    {
        let message = LxMessage::from_paper_uri(uri, decrypt_fn)?;
        if let Some(ref cb) = self.delivery_callback {
            cb(&message);
        }
        Ok(message)
    }

    /// Register a router-wide callback fired on every inbound message delivery.
    ///
    /// Python reference: `LXMRouter.register_delivery_callback` — LXMRouter.py:358-359.
    pub fn register_delivery_callback<F>(&mut self, callback: F)
    where
        F: Fn(&LxMessage) + Send + 'static,
    {
        self.delivery_callback = Some(Box::new(callback));
    }

    /// Deliver an already decoded inbound message to the application callback.
    ///
    /// Reticulum adapters perform transport decryption, source-key lookup,
    /// signature/stamp validation and deduplication before calling this method.
    /// Keeping the final callback dispatch public lets embedding applications
    /// use the same router surface as `lxmd-rs`.
    pub fn deliver_inbound(&self, message: &LxMessage) -> bool {
        let Some(callback) = self.delivery_callback.as_ref() else {
            return false;
        };
        callback(message);
        true
    }

    /// Return peer destination hashes that are due for sync and mark each as
    /// `LinkEstablishing` so concurrent calls don't double-schedule.
    ///
    /// Python's `LXMRouter.sync_peers()` drives network I/O directly; in Rust
    /// network I/O lives outside the router, so callers (e.g. `lxmd`) feed this
    /// list into whatever sync task manages link establishment.
    pub fn sync_peers(&mut self) -> Vec<[u8; 16]> {
        let mut due = Vec::new();
        for (hash, peer) in self.peers.iter_mut() {
            if peer.alive
                && peer.state == PeerState::Idle
                && peer.unhandled_messages() > 0
                && peer.should_sync()
            {
                peer.begin_sync();
                due.push(*hash);
            }
        }
        due
    }

    pub fn add_peer(&mut self, peer: LxmPeer) -> bool {
        if self.peers.len() >= self.config.max_peers {
            return false;
        }
        self.peers.insert(peer.destination_hash, peer);
        if self.storage_authoritative
            && let Err(error) = self.checkpoint_peers()
        {
            tracing::warn!(%error, "failed to persist added propagation peer");
        }
        true
    }

    /// Add a peer from announce data.
    ///
    /// The peer is added only when autopeer is enabled, the router is below
    /// `max_peers`, and the announce hop count is within `autopeer_maxdepth`.
    pub fn autopeer(&mut self, candidate: AutopeerCandidate) -> bool {
        let AutopeerCandidate {
            destination_hash,
            timebase,
            transfer_limit,
            sync_limit,
            stamp_cost,
            stamp_flexibility,
            peering_cost,
            hops,
        } = candidate;

        if !self.config.autopeer {
            return false;
        }
        // Python LXMRouter.peer() (LXMRouter.py:1896-1901): a peering cost
        // above our accepted maximum refuses the peering — and breaks an
        // existing one — before any PoW could be attempted.
        if peering_cost.unwrap_or(0) > self.config.ext.max_peering_cost {
            if self.peers.contains_key(&destination_hash) {
                self.unpeer(&destination_hash);
            }
            return false;
        }
        if self.peers.contains_key(&destination_hash) {
            return false;
        }
        if let Some(h) = hops
            && h as usize > self.config.ext.autopeer_maxdepth
        {
            return false;
        }

        let peer = LxmPeer::from_announce(
            destination_hash,
            timebase,
            transfer_limit,
            sync_limit,
            stamp_cost,
            stamp_flexibility,
            peering_cost,
        );
        self.add_peer(peer)
    }

    pub fn remove_peer(&mut self, destination_hash: &[u8; 16]) {
        self.peers.remove(destination_hash);
        if self.storage_authoritative {
            let _ = self.checkpoint_peers();
        }
    }

    /// Remove a peer from both the active and static peer sets.
    pub fn unpeer(&mut self, destination_hash: &[u8; 16]) {
        self.peers.remove(destination_hash);
        self.static_peers.retain(|h| h != destination_hash);
        if self.storage_authoritative {
            let _ = self.checkpoint_peers();
        }
    }

    /// Get a cached outbound stamp cost, or `None` if missing or expired.
    pub fn get_stamp_cost(&self, destination_hash: &[u8; 16]) -> Option<u8> {
        if self.storage_authoritative {
            let entry = self.storage.stamp_cost(destination_hash).ok().flatten()?;
            return (now_f64() - entry.recorded_at < STAMP_COST_EXPIRY as f64)
                .then_some(entry.cost);
        }
        let entry = self.outbound_stamp_costs.get(destination_hash)?;
        let now = now_f64();
        if now - entry.recorded_at < STAMP_COST_EXPIRY as f64 {
            Some(entry.cost)
        } else {
            None
        }
    }

    pub fn set_stamp_cost(&mut self, destination_hash: [u8; 16], cost: u8) {
        let now = now_f64();
        if self.storage_authoritative {
            let _ = self.storage.upsert_stamp_cost(StoredStampCost {
                destination_hash,
                cost,
                recorded_at: now,
            });
            return;
        }
        self.outbound_stamp_costs.insert(
            destination_hash,
            StampCostEntry {
                cost,
                recorded_at: now,
            },
        );
    }

    pub fn remove_stamp_cost(&mut self, destination_hash: &[u8; 16]) -> bool {
        if self.storage_authoritative {
            self.storage
                .remove_stamp_cost(destination_hash)
                .unwrap_or(false)
        } else {
            self.outbound_stamp_costs.remove(destination_hash).is_some()
        }
    }

    pub fn set_propagation_enabled(&mut self, enabled: bool) {
        self.config.propagation_enabled = enabled;
    }

    /// Set the singular outbound propagation node used for `PROPAGATED`
    /// message delivery.
    pub fn set_outbound_propagation_node(&mut self, destination_hash: Option<[u8; 16]>) {
        self.outbound_propagation_node = destination_hash;
    }

    /// Mark pending messages due after a destination announce.
    ///
    /// Python's delivery announce handler sets `next_delivery_attempt = time.time()`
    /// and triggers outbound processing. Rust tracks the previous attempt time,
    /// so setting it beyond the retry window makes the message eligible on the
    /// next scheduler tick.
    pub fn trigger_outbound_for_delivery_announce(&mut self, destination_hash: [u8; 16]) -> usize {
        let now = now_f64();
        let due_now = now - DELIVERY_RETRY_WAIT as f64;
        let mut triggered = 0;
        for message in &mut self.pending_outbound {
            if message.destination_hash == destination_hash {
                message.last_delivery_attempt = due_now;
                message.next_delivery_attempt = now;
                triggered += 1;
            }
        }
        triggered
    }

    /// Mark pending propagated messages due after the configured propagation node announces.
    ///
    /// Mirrors LXMF 0.9.8's propagation announce handler. The announce app_data
    /// must be a valid propagation-node announce before any retry backoff is
    /// cleared.
    pub fn trigger_outbound_for_propagation_node_announce(
        &mut self,
        destination_hash: [u8; 16],
        app_data: &[u8],
    ) -> usize {
        if self.outbound_propagation_node != Some(destination_hash) {
            return 0;
        }
        if crate::handlers::parse_pn_announce_data(app_data).is_none() {
            return 0;
        }

        let now = now_f64();
        let due_now = now - DELIVERY_RETRY_WAIT as f64;
        let mut triggered = 0;
        for message in &mut self.pending_outbound {
            if message.method == DeliveryMethod::Propagated {
                message.last_delivery_attempt = due_now;
                message.next_delivery_attempt = now;
                triggered += 1;
            }
        }
        triggered
    }

    pub fn set_autopeer(&mut self, enabled: bool) {
        self.config.autopeer = enabled;
    }

    pub fn set_max_peers(&mut self, max: usize) {
        self.config.max_peers = max;
    }

    /// Propagation storage limit in kilobytes.
    pub fn set_propagation_limit(&mut self, limit_kb: usize) {
        self.config.propagation_limit_kb = limit_kb;
    }

    pub fn set_stamp_requirements(&mut self, cost: u8, flex: u8) {
        self.config.propagation_stamp_cost = cost;
        self.config.propagation_stamp_flex = flex;
    }

    /// Build propagation-node announce app_data (msgpack).
    ///
    /// Python reference: LXMRouter.get_propagation_node_app_data — LXMRouter.py:306-318.
    pub fn get_propagation_node_app_data(&self) -> Vec<u8> {
        use std::time::{SystemTime, UNIX_EPOCH};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let mut metadata = std::collections::HashMap::new();
        if let Some(ref name) = self.config.ext.name {
            metadata.insert(0u8, name.as_bytes().to_vec());
        }

        let data = crate::handlers::PropagationNodeAnnounceData {
            legacy: false,
            node_state: self.config.propagation_enabled && !self.config.ext.from_static_only,
            timebase: now,
            transfer_limit: self.config.propagation_limit_kb as u64,
            sync_limit: self.config.sync_limit_kb as u64,
            stamp_cost: self.config.propagation_stamp_cost,
            stamp_flex: self.config.propagation_stamp_flex,
            peering_cost: self.config.ext.peering_cost,
            metadata,
        };

        crate::handlers::get_propagation_node_app_data(&data)
    }

    /// Validate a PoW stamp on an incoming message.
    pub fn validate_stamp(
        &self,
        message_hash: &[u8; 32],
        stamp: &[u8; 32],
        required_cost: u8,
    ) -> bool {
        stamper::validate_stamp(
            message_hash,
            stamp,
            required_cost,
            STAMP_WORKBLOCK_EXPAND_ROUNDS,
        )
    }

    /// Validate a stamp, accepting a matching ticket hash as a bypass.
    ///
    /// A ticket is matched by comparing the first 16 bytes of
    /// `SHA-256(ticket.token || message_id)` against the stamp prefix;
    /// otherwise falls back to PoW validation.
    pub fn validate_stamp_with_tickets(
        &self,
        message_id: &[u8; 32],
        stamp: &[u8],
        required_cost: u8,
        destination_hash: &[u8; 16],
    ) -> bool {
        let now = now_f64();
        for ticket in self.ticket_store.all() {
            if &ticket.destination_hash != destination_hash || !ticket.is_valid(now) {
                continue;
            }
            let mut material = Vec::with_capacity(16 + 32);
            material.extend_from_slice(&ticket.token);
            material.extend_from_slice(message_id);
            let expected = rns_crypto::sha::truncated_hash(&material);
            if stamp == expected.as_ref() {
                return true;
            }
        }

        let Ok(pow_stamp) = <&[u8; 32]>::try_from(stamp) else {
            return false;
        };
        self.validate_stamp(message_id, pow_stamp, required_cost)
    }

    /// Called when a propagation transfer resource completes.
    pub fn handle_resource_concluded(&mut self, peer_hash: &[u8; 16], success: bool) {
        if let Some(peer) = self.peers.get_mut(peer_hash) {
            if success {
                if let Some(transferring) = peer.currently_transferring_messages.take() {
                    peer.outgoing += transferring.len() as u64;
                    peer.offered += transferring.len() as u64;
                }
                peer.heard();
                peer.sync_complete();
            } else {
                peer.sync_failed();
            }
        }
    }

    /// Summarise propagation-node state for a control status request.
    ///
    /// Python reference: LXMRouter.compile_stats.
    pub fn control_status(&self) -> Option<NodeStats> {
        self.control_status_at(now_f64())
    }

    /// Summarise propagation-node state using an explicit clock value.
    pub fn control_status_at(&self, now: f64) -> Option<NodeStats> {
        if !self.config.propagation_enabled {
            return None;
        }

        let peer_stats: HashMap<[u8; 16], PeerStats> = self
            .peers
            .iter()
            .map(|(hash, peer)| {
                (
                    *hash,
                    PeerStats {
                        peer_type: if self.static_peers.contains(hash) {
                            "static".to_string()
                        } else {
                            "discovered".to_string()
                        },
                        state: peer.state as u8,
                        alive: peer.alive,
                        last_heard: peer.last_heard,
                        next_sync_attempt: peer.next_sync_attempt,
                        last_sync_attempt: peer.last_sync_attempt,
                        sync_backoff: peer.sync_backoff,
                        peering_timebase: peer.peering_timebase,
                        link_establishment_rate: peer.link_establishment_rate,
                        sync_transfer_rate: peer.sync_transfer_rate,
                        transfer_limit: peer.propagation_transfer_limit,
                        sync_limit: peer.propagation_sync_limit,
                        stamp_cost: peer.stamp_cost,
                        stamp_cost_flexibility: peer.stamp_cost_flexibility,
                        peering_cost: peer.peering_cost,
                        peering_key: peer
                            .peering_key
                            .as_ref()
                            .map(|(_, value)| u64::from(*value)),
                        rx_bytes: peer.rx_bytes,
                        tx_bytes: peer.tx_bytes,
                        offered: peer.offered,
                        outgoing: peer.outgoing,
                        incoming: peer.incoming,
                        unhandled: peer.unhandled_messages(),
                    },
                )
            })
            .collect();

        Some(NodeStats {
            uptime: self.propagation_start_time.map(|t| now - t).unwrap_or(0.0),
            delivery_limit: self.config.delivery_limit_kb,
            propagation_limit: self.config.propagation_limit_kb,
            sync_limit: self.config.sync_limit_kb,
            stamp_cost: self.config.propagation_stamp_cost,
            stamp_flex: self.config.propagation_stamp_flex,
            peering_cost: self.config.ext.peering_cost,
            max_peering_cost: self.config.ext.max_peering_cost,
            autopeer_maxdepth: self.config.ext.autopeer_maxdepth,
            from_static_only: self.config.ext.from_static_only,
            message_count: self.propagation_store.len(),
            message_size: self.propagation_store.total_size(),
            storage_limit: self.config.ext.message_storage_limit,
            total_peers: self.peers.len(),
            max_peers: self.config.max_peers,
            client_messages_received: self.client_propagation_messages_received,
            client_messages_served: self.client_propagation_messages_served,
            unpeered_incoming: self.unpeered_propagation_incoming,
            unpeered_rx_bytes: self.unpeered_propagation_rx_bytes,
            peer_stats,
        })
    }

    pub fn clean_throttled_peers(&mut self) {
        let now = now_f64();
        self.throttled_peers.retain(|_, expiry| now < *expiry);
    }

    pub fn is_peer_throttled(&self, peer_hash: &[u8; 16]) -> bool {
        if let Some(expiry) = self.throttled_peers.get(peer_hash) {
            now_f64() < *expiry
        } else {
            false
        }
    }

    /// Throttle a peer for [`PN_STAMP_THROTTLE`] seconds.
    pub fn throttle_peer(&mut self, peer_hash: [u8; 16]) {
        self.throttled_peers
            .insert(peer_hash, now_f64() + PN_STAMP_THROTTLE as f64);
    }

    pub fn unthrottle_peer(&mut self, peer_hash: &[u8; 16]) -> bool {
        self.throttled_peers.remove(peer_hash).is_some()
    }

    /// Drop idle, non-static peers with the lowest acceptance rates.
    ///
    /// Python reference: LXMRouter.rotate_peers.
    pub fn rotate_peers(&mut self) {
        let rotation_headroom = (self.config.max_peers * ROTATION_HEADROOM_PCT / 100).max(1);
        let required_drops = self.peers.len() as isize
            - (self.config.max_peers as isize - rotation_headroom as isize);

        if required_drops <= 0 || self.peers.len() <= 1 {
            return;
        }

        // Postpone rotation while a full headroom of peers has never been sync-tested.
        let untested_count = self
            .peers
            .values()
            .filter(|p| p.last_sync_attempt == 0.0)
            .count();
        if untested_count >= rotation_headroom {
            return;
        }

        let mut drop_candidates: Vec<([u8; 16], f64)> = self
            .peers
            .iter()
            .filter(|(hash, peer)| {
                !self.static_peers.contains(hash)
                    && peer.state == PeerState::Idle
                    && peer.offered > 0
            })
            .map(|(hash, peer)| (*hash, peer.acceptance_rate()))
            .collect();

        drop_candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let drop_count = (required_drops as usize).min(drop_candidates.len());
        for (hash, ar) in drop_candidates.into_iter().take(drop_count) {
            if ar < ROTATION_AR_MAX {
                self.unpeer(&hash);
            }
        }
    }

    /// Drain pending outbound messages into [`OutboundAction`]s.
    ///
    /// A delivery that later fails externally (e.g. unknown destination key)
    /// should be re-queued via [`send`][Self::send] with `delivery_attempts`
    /// incremented.
    #[tracing::instrument(
        level = "debug",
        name = "router.process_outbound",
        skip_all,
        fields(pending_count = self.pending_outbound.len()),
    )]
    pub fn process_outbound(&mut self) -> Vec<OutboundAction> {
        self.process_outbound_inner(DirectOutboundHandling::EmitLegacyAction)
    }

    fn process_outbound_without_direct(&mut self) -> Vec<OutboundAction> {
        self.process_outbound_inner(DirectOutboundHandling::LeavePending)
    }

    fn process_outbound_inner(
        &mut self,
        direct_handling: DirectOutboundHandling,
    ) -> Vec<OutboundAction> {
        if !self.config.ext.processing_outbound {
            return Vec::new();
        }
        self.hydrate_ready_outbound();

        let mut actions = Vec::new();
        let mut processed = 0usize;

        let mut i = 0;
        while i < self.pending_outbound.len() {
            if let Some(limit) = self.config.ext.processing_limit
                && processed >= limit
            {
                break;
            }

            let msg = &self.pending_outbound[i];

            let now = now_f64();

            // Python fails only when delivery_attempts > MAX (outer guard is
            // `<= MAX`, LXMRouter.py:2597/2671); match that boundary exactly.
            if msg.delivery_attempts > MAX_DELIVERY_ATTEMPTS {
                let mut msg = self.pending_outbound.remove(i);
                msg.mark_failed();
                actions.push(OutboundAction::Failed(msg));
                processed += 1;
                continue;
            }

            // Python LXMF gates on an absolute `next_delivery_attempt`. Honor an
            // explicit deadline when set (path request -> now+7s, etc.);
            // otherwise fall back to the legacy
            // last_delivery_attempt + DELIVERY_RETRY_WAIT (10s) rule.
            let due_at = if msg.next_delivery_attempt > 0.0 {
                msg.next_delivery_attempt
            } else if msg.last_delivery_attempt > 0.0 {
                msg.last_delivery_attempt + DELIVERY_RETRY_WAIT as f64
            } else {
                0.0
            };
            if now < due_at {
                i += 1;
                continue;
            }

            let age = now - msg.timestamp;
            if age > MESSAGE_EXPIRY as f64 {
                let mut msg = self.pending_outbound.remove(i);
                msg.mark_failed();
                actions.push(OutboundAction::Expired(msg));
                processed += 1;
                continue;
            }

            // State transitions match Python LXMessage.py:476-499:
            //   Opportunistic -> Sent immediately (single packet, fire-and-forget).
            //   Direct / Propagated -> Sending (multi-step).
            match msg.method {
                DeliveryMethod::Direct => {
                    match direct_handling {
                        DirectOutboundHandling::EmitLegacyAction => {
                            let mut msg = self.pending_outbound.remove(i);
                            msg.mark_sending();
                            let dest_hash = msg.destination_hash;
                            actions.push(OutboundAction::DeliverDirect {
                                message: msg,
                                dest_hash,
                            });
                            processed += 1;
                            // The next element has shifted into index i, so do not advance.
                        }
                        DirectOutboundHandling::LeavePending => {
                            i += 1;
                        }
                    }
                }
                DeliveryMethod::Propagated => {
                    if let Some(peer_hash) = self.outbound_propagation_node {
                        let mut msg = self.pending_outbound.remove(i);
                        msg.mark_sending();
                        actions.push(OutboundAction::DeliverPropagated {
                            message: msg,
                            prop_hash: peer_hash,
                        });
                        processed += 1;
                    } else {
                        i += 1;
                    }
                }
                DeliveryMethod::Opportunistic => {
                    let mut msg = self.pending_outbound.remove(i);
                    msg.mark_sent();
                    msg.progress = 0.50;
                    let dest_hash = msg.destination_hash;
                    actions.push(OutboundAction::DeliverOpportunistic {
                        message: msg,
                        dest_hash,
                    });
                    processed += 1;
                }
                _ => {
                    i += 1;
                }
            }
        }

        self.sync_authoritative_outbound(&actions);
        actions
    }

    /// Process outbound messages while keeping Direct deliveries in the router
    /// queue until LinkDeliveryManager reports a terminal result.
    ///
    /// This mirrors Python's `LXMRouter.process_outbound` ownership model for
    /// Direct messages while preserving [`Self::process_outbound`] for older
    /// embedders that still take ownership of Direct messages themselves.
    pub fn process_outbound_with_direct<F>(&mut self, mut direct_input: F) -> Vec<OutboundAction>
    where
        F: FnMut(&LxMessage, f64) -> DirectDeliveryPlanInput,
    {
        if !self.config.ext.processing_outbound {
            return Vec::new();
        }
        self.hydrate_ready_outbound();

        let mut actions = Vec::new();
        let mut processed = 0usize;
        let mut i = 0;

        while i < self.pending_outbound.len() {
            if let Some(limit) = self.config.ext.processing_limit
                && processed >= limit
            {
                break;
            }

            let now = now_f64();

            if self.pending_outbound[i].delivery_attempts > MAX_DELIVERY_ATTEMPTS {
                let mut msg = self.pending_outbound.remove(i);
                msg.mark_failed();
                actions.push(OutboundAction::Failed(msg));
                processed += 1;
                continue;
            }

            let age = now - self.pending_outbound[i].timestamp;
            if age > MESSAGE_EXPIRY as f64 {
                let mut msg = self.pending_outbound.remove(i);
                msg.mark_failed();
                actions.push(OutboundAction::Expired(msg));
                processed += 1;
                continue;
            }

            match self.pending_outbound[i].method {
                DeliveryMethod::Direct => {
                    let input = direct_input(&self.pending_outbound[i], now);
                    if input.reusable_link == DirectReusableLinkState::None {
                        let msg = &self.pending_outbound[i];
                        let due_at = if msg.next_delivery_attempt > 0.0 {
                            msg.next_delivery_attempt
                        } else if msg.last_delivery_attempt > 0.0 {
                            msg.last_delivery_attempt + DELIVERY_RETRY_WAIT as f64
                        } else {
                            0.0
                        };
                        if now < due_at {
                            i += 1;
                            continue;
                        }
                    }

                    if self.pending_outbound[i].state != MessageState::Sending {
                        self.pending_outbound[i].mark_sending();
                    }
                    let plan = plan_direct_delivery(&mut self.pending_outbound[i], input, now);
                    match plan {
                        DirectDeliveryPlan::Fail => {
                            let mut msg = self.pending_outbound.remove(i);
                            msg.mark_failed();
                            actions.push(OutboundAction::Failed(msg));
                            processed += 1;
                            continue;
                        }
                        DirectDeliveryPlan::DeferTerminalFailure => {
                            processed += 1;
                            i += 1;
                        }
                        _ => {
                            let msg = self.pending_outbound[i].clone();
                            let dest_hash = msg.destination_hash;
                            actions.push(OutboundAction::PlanDirect {
                                message: msg,
                                dest_hash,
                                plan,
                            });
                            processed += 1;
                            i += 1;
                        }
                    }
                }
                DeliveryMethod::Propagated => {
                    let msg = &self.pending_outbound[i];
                    let due_at = if msg.next_delivery_attempt > 0.0 {
                        msg.next_delivery_attempt
                    } else if msg.last_delivery_attempt > 0.0 {
                        msg.last_delivery_attempt + DELIVERY_RETRY_WAIT as f64
                    } else {
                        0.0
                    };
                    if now < due_at {
                        i += 1;
                        continue;
                    }

                    if let Some(peer_hash) = self.outbound_propagation_node {
                        let mut msg = self.pending_outbound.remove(i);
                        msg.mark_sending();
                        actions.push(OutboundAction::DeliverPropagated {
                            message: msg,
                            prop_hash: peer_hash,
                        });
                        processed += 1;
                    } else {
                        i += 1;
                    }
                }
                DeliveryMethod::Opportunistic => {
                    let msg = &self.pending_outbound[i];
                    let due_at = if msg.next_delivery_attempt > 0.0 {
                        msg.next_delivery_attempt
                    } else if msg.last_delivery_attempt > 0.0 {
                        msg.last_delivery_attempt + DELIVERY_RETRY_WAIT as f64
                    } else {
                        0.0
                    };
                    if now < due_at {
                        i += 1;
                        continue;
                    }

                    let mut msg = self.pending_outbound.remove(i);
                    msg.mark_sent();
                    msg.progress = 0.50;
                    let dest_hash = msg.destination_hash;
                    actions.push(OutboundAction::DeliverOpportunistic {
                        message: msg,
                        dest_hash,
                    });
                    processed += 1;
                }
                DeliveryMethod::Paper => {
                    i += 1;
                }
            }
        }

        self.sync_authoritative_outbound(&actions);
        actions
    }

    fn sync_authoritative_outbound(&mut self, actions: &[OutboundAction]) {
        if !self.storage_authoritative {
            return;
        }
        let pending = self.pending_outbound.clone();
        for message in &pending {
            self.persist_outbound_message(message, false);
        }
        for action in actions {
            let (message, terminal_or_handed_off) = match action {
                OutboundAction::Failed(message) | OutboundAction::Expired(message) => {
                    (message, true)
                }
                OutboundAction::DeliverDirect { message, .. }
                | OutboundAction::DeliverPropagated { message, .. }
                | OutboundAction::DeliverOpportunistic { message, .. } => (message, true),
                OutboundAction::PlanDirect { message, .. } => (message, false),
            };
            if !terminal_or_handed_off {
                continue;
            }
            if let Some(message_id) = message.message_id.or(message.hash) {
                let _ = self.storage.remove_outbound_message(&message_id);
                self.outbound_callbacks.remove(&message_id);
            }
        }
    }

    pub fn cull_stamp_costs(&mut self) {
        let now = now_f64();
        if self.storage_authoritative {
            let _ = self
                .storage
                .cull_stamp_costs_before(now - STAMP_COST_EXPIRY as f64);
            let _ = self.storage.cull_tickets(now - TICKET_GRACE as f64);
            return;
        }
        self.outbound_stamp_costs
            .retain(|_, e| now - e.recorded_at < STAMP_COST_EXPIRY as f64);
    }

    pub fn cull_propagation(&mut self) {
        self.propagation_store.cull_expired(MESSAGE_EXPIRY);
        if let Some(limit) = self.config.ext.message_storage_limit {
            self.propagation_store.cull_by_weight(limit);
        }
    }

    /// Send single-packet opportunistic actions via the configured transport.
    ///
    /// Direct and Propagated actions require a Reticulum link and are handled by
    /// `LinkDeliveryManager` / propagation helpers in the embedding runtime.
    pub fn execute_actions(&mut self, actions: Vec<OutboundAction>) {
        self.execute_actions_with_encryptor(actions, |dest_hash, _plaintext| {
            Err(MessageError::PackFailed(format!(
                "no destination encryptor configured for opportunistic delivery to {}",
                hex::encode(dest_hash)
            )))
        });
    }

    /// Send single-packet opportunistic actions with caller-supplied
    /// destination encryption.
    ///
    /// Python encrypts Opportunistic LXMF packet payloads with the recipient
    /// destination identity before handing bytes to Reticulum. Core cannot
    /// infer destination keys, so embeddings must provide the encryptor.
    pub fn execute_actions_with_encryptor<F>(
        &mut self,
        actions: Vec<OutboundAction>,
        mut encrypt_fn: F,
    ) where
        F: FnMut([u8; 16], &[u8]) -> Result<Vec<u8>, MessageError>,
    {
        let transport_tx = match &self.transport_tx {
            Some(tx) => tx.clone(),
            None => return,
        };

        for action in actions {
            match action {
                OutboundAction::DeliverOpportunistic {
                    mut message,
                    dest_hash,
                } => {
                    match message
                        .pack_opportunistic_encrypted(|plaintext| encrypt_fn(dest_hash, plaintext))
                    {
                        Ok(packet_payload) => {
                            // Python LXMessage.__as_packet strips the destination
                            // hash before encryption because the RNS packet
                            // header already carries it.
                            if rns_runtime::application::try_send_pre_encrypted_packet_on_transport(
                                &transport_tx,
                                dest_hash,
                                &packet_payload,
                            )
                            .is_ok()
                                && message.state == MessageState::Sending
                            {
                                message.mark_sent();
                                message.progress = 1.0;
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                dest = %hex::encode(dest_hash),
                                error = %err,
                                "cannot execute opportunistic LXMF action"
                            );
                        }
                    }
                }
                OutboundAction::DeliverDirect { .. } | OutboundAction::PlanDirect { .. } => {
                    tracing::warn!(
                        "Direct LXMF delivery requires LinkDeliveryManager; action left for embedding runtime"
                    );
                }
                OutboundAction::DeliverPropagated { .. } => {
                    // Requires a link to a propagation node; handled outside this layer.
                }
                OutboundAction::Failed(_) | OutboundAction::Expired(_) => {}
            }
        }
    }

    /// Advance one scheduler tick: drain outbound, then run periodic jobs.
    pub fn tick(&mut self) {
        self.processing_count += 1;

        self.process_deferred_stamps();
        let actions = self.process_outbound_without_direct();
        if !actions.is_empty() {
            self.execute_actions(actions);
        }

        self.run_periodic_jobs();
    }

    /// Advance one scheduler tick using caller-supplied destination encryption
    /// for Opportunistic packet actions.
    pub fn tick_with_encryptor<F>(&mut self, encrypt_fn: F)
    where
        F: FnMut([u8; 16], &[u8]) -> Result<Vec<u8>, MessageError>,
    {
        self.processing_count += 1;

        self.process_deferred_stamps();
        let actions = self.process_outbound_without_direct();
        if !actions.is_empty() {
            self.execute_actions_with_encryptor(actions, encrypt_fn);
        }

        self.run_periodic_jobs();
    }

    /// Drive the periodic job machinery for embedders that process outbound
    /// themselves via `process_outbound_with_direct` instead of calling
    /// [`Self::tick`]. Without this the jobloop counters never advance and
    /// transient-cache cleaning, store culls, and peer rotation never run.
    /// Time-gated to Python's `PROCESSING_INTERVAL` so callers may invoke it
    /// every loop pass regardless of their tick rate (lxmd: 4 s, Ratspeak:
    /// 500 ms) without skewing the jobloop cadences.
    pub fn run_jobs_tick(&mut self) {
        let now = now_f64();
        if now - self.last_jobs_tick < PROCESSING_INTERVAL as f64 * 0.9 {
            return;
        }
        self.last_jobs_tick = now;
        self.processing_count += 1;
        self.run_periodic_jobs();
    }

    fn run_periodic_jobs(&mut self) {
        // Job cadences match the Python LXMRouter jobloop.
        if self.processing_count.is_multiple_of(JOB_TRANSIENT_INTERVAL) {
            if let Err(error) = self.cull_transient_ids(now_f64() as i64) {
                tracing::warn!(%error, "failed to cull transient IDs");
            }
        }
        if self.processing_count.is_multiple_of(JOB_STORE_INTERVAL)
            && self.config.propagation_enabled
        {
            self.cull_propagation();
        }
        if self.processing_count.is_multiple_of(JOB_PEERSYNC_INTERVAL) {
            self.clean_throttled_peers();
        }
        if self.processing_count.is_multiple_of(JOB_ROTATE_INTERVAL)
            && self.config.propagation_enabled
        {
            self.rotate_peers();
        }
    }

    /// Get summary statistics.
    pub fn stats(&self) -> RouterStats {
        let pending_outbound = if self.storage_authoritative {
            self.storage.outbound_count(Some(false)).unwrap_or_default()
        } else {
            self.pending_outbound.len()
        };
        let pending_deferred_stamps = if self.storage_authoritative {
            self.storage.outbound_count(Some(true)).unwrap_or_default()
        } else {
            self.pending_deferred_stamps.len()
        };
        RouterStats {
            pending_outbound,
            pending_deferred_stamps,
            peers: self.peers.len(),
            propagation_entries: self.propagation_store.len(),
            propagation_size: self.propagation_store.total_size(),
            stamp_costs_cached: if self.storage_authoritative {
                self.storage.stamp_cost_count().unwrap_or_default()
            } else {
                self.outbound_stamp_costs.len()
            },
        }
    }
}

/// Action to take on an outbound message.
#[derive(Debug)]
pub enum OutboundAction {
    /// Exhausted delivery attempts.
    Failed(LxMessage),
    /// Exceeded [`MESSAGE_EXPIRY`].
    Expired(LxMessage),
    DeliverDirect {
        message: LxMessage,
        dest_hash: [u8; 16],
    },
    /// Direct delivery planned without removing the message from
    /// `pending_outbound`.
    PlanDirect {
        message: LxMessage,
        dest_hash: [u8; 16],
        plan: DirectDeliveryPlan,
    },
    DeliverPropagated {
        message: LxMessage,
        prop_hash: [u8; 16],
    },
    /// Small enough for single-packet delivery.
    DeliverOpportunistic {
        message: LxMessage,
        dest_hash: [u8; 16],
    },
}

/// Router statistics.
#[derive(Debug)]
pub struct RouterStats {
    pub pending_outbound: usize,
    pub pending_deferred_stamps: usize,
    pub peers: usize,
    pub propagation_entries: usize,
    pub propagation_size: usize,
    pub stamp_costs_cached: usize,
}

/// Payload-free status for a queued outbound message.
#[derive(Debug, Clone, Copy)]
pub struct OutboundSummary {
    pub message_id: Option<[u8; 32]>,
    pub destination_hash: [u8; 16],
    pub state: MessageState,
    pub method: DeliveryMethod,
    pub delivery_attempts: u32,
    pub last_delivery_attempt: f64,
    pub next_delivery_attempt: f64,
    pub progress: f64,
}

/// Per-peer stats for control status.
#[derive(Debug, Clone)]
pub struct PeerStats {
    pub peer_type: String,
    pub state: u8,
    pub alive: bool,
    pub last_heard: f64,
    pub next_sync_attempt: f64,
    pub last_sync_attempt: f64,
    pub sync_backoff: f64,
    pub peering_timebase: f64,
    pub link_establishment_rate: f64,
    pub sync_transfer_rate: f64,
    pub transfer_limit: Option<f64>,
    pub sync_limit: Option<f64>,
    pub stamp_cost: Option<u8>,
    pub stamp_cost_flexibility: Option<u8>,
    pub peering_cost: u8,
    pub peering_key: Option<u64>,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub offered: u64,
    pub outgoing: u64,
    pub incoming: u64,
    pub unhandled: u32,
}

/// Propagation node stats.
#[derive(Debug)]
pub struct NodeStats {
    pub uptime: f64,
    pub delivery_limit: usize,
    pub propagation_limit: usize,
    pub sync_limit: usize,
    pub stamp_cost: u8,
    pub stamp_flex: u8,
    pub peering_cost: u8,
    pub max_peering_cost: u8,
    pub autopeer_maxdepth: usize,
    pub from_static_only: bool,
    pub message_count: usize,
    pub message_size: usize,
    pub storage_limit: Option<usize>,
    pub total_peers: usize,
    pub max_peers: usize,
    pub client_messages_received: u64,
    pub client_messages_served: u64,
    pub unpeered_incoming: u64,
    pub unpeered_rx_bytes: u64,
    pub peer_stats: HashMap<[u8; 16], PeerStats>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn durable_outbound_message(method: DeliveryMethod) -> LxMessage {
        let mut message = LxMessage::new([0xA1; 16], [0xB1; 16], "durable", "outbound", method);
        message
            .sign(&rns_crypto::ed25519::Ed25519PrivateKey::generate())
            .unwrap();
        message
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_outbound_queue_recovers_and_loads_only_when_ready() {
        use crate::storage::spawn_sqlite_storage_actor;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("outbound-restart.sqlite");
        let mut message = durable_outbound_message(DeliveryMethod::Direct);
        message.delivery_attempts = 2;
        message.progress = 0.4;
        message.next_delivery_attempt = now_f64() + 3_600.0;
        let message_id = message.message_id.unwrap();
        {
            let storage = spawn_sqlite_storage_actor(path.clone()).unwrap();
            let mut router =
                LxmRouter::with_shared_storage_backend(RouterConfig::default(), storage);
            router.send(message);
            assert!(router.pending_outbound.is_empty());
            assert_eq!(router.stats().pending_outbound, 1);
        }

        let mut storage = spawn_sqlite_storage_actor(path).unwrap();
        let mut router =
            LxmRouter::with_shared_storage_backend(RouterConfig::default(), storage.clone());
        assert!(router.pending_outbound.is_empty());
        assert_eq!(router.outbound_summaries(1)[0].message_id, Some(message_id));
        assert!(router.process_outbound().is_empty());
        let mut stored = storage.outbound_message(&message_id).unwrap().unwrap();
        stored.metadata.next_delivery_attempt = 0.0;
        storage.update_outbound_delivery(&stored.metadata).unwrap();
        let actions = router.process_outbound();
        let OutboundAction::DeliverDirect { message, .. } = &actions[0] else {
            panic!("recovered direct message must be delivered");
        };
        assert_eq!(message.delivery_attempts, 2);
        assert_eq!(message.progress, 0.4);
        assert_eq!(router.stats().pending_outbound, 0);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_outbound_expiry_attempts_and_callbacks_are_terminal() {
        use crate::storage::spawn_sqlite_storage_actor;
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let directory = tempfile::tempdir().unwrap();
        let storage = spawn_sqlite_storage_actor(directory.path().join("terminal.sqlite")).unwrap();
        let mut router = LxmRouter::with_shared_storage_backend(RouterConfig::default(), storage);

        let mut expired = durable_outbound_message(DeliveryMethod::Direct);
        expired.timestamp = now_f64() - MESSAGE_EXPIRY as f64 - 1.0;
        router.send(expired);
        assert!(matches!(
            router.process_outbound().as_slice(),
            [OutboundAction::Expired(_)]
        ));

        let mut exhausted = durable_outbound_message(DeliveryMethod::Direct);
        exhausted.delivery_attempts = MAX_DELIVERY_ATTEMPTS + 1;
        router.send(exhausted);
        let exhausted_actions = router.process_outbound();
        assert!(
            matches!(exhausted_actions.as_slice(), [OutboundAction::Failed(_)]),
            "unexpected actions: {exhausted_actions:?}"
        );

        let failures = Arc::new(AtomicUsize::new(0));
        let mut callback_message = durable_outbound_message(DeliveryMethod::Direct);
        let callback_id = callback_message.message_id.unwrap();
        let callback_failures = failures.clone();
        callback_message.callbacks.on_failed = Some(Arc::new(move |_| {
            callback_failures.fetch_add(1, Ordering::SeqCst);
        }));
        router.send(callback_message);
        assert!(router.mark_outbound_failed(&callback_id));
        assert_eq!(failures.load(Ordering::SeqCst), 1);
        assert_eq!(router.stats().pending_outbound, 0);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_deferred_stamp_recovers_after_restart() {
        use crate::storage::spawn_sqlite_storage_actor;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("deferred-restart.sqlite");
        let mut message = durable_outbound_message(DeliveryMethod::Direct);
        message.stamp_cost = Some(0);
        {
            let storage = spawn_sqlite_storage_actor(path.clone()).unwrap();
            let mut router =
                LxmRouter::with_shared_storage_backend(RouterConfig::default(), storage);
            assert!(router.defer_stamp(message).is_none());
            assert_eq!(router.stats().pending_deferred_stamps, 1);
        }

        let storage = spawn_sqlite_storage_actor(path).unwrap();
        let mut router = LxmRouter::with_shared_storage_backend(RouterConfig::default(), storage);
        router.process_deferred_stamps();
        assert_eq!(router.stats().pending_deferred_stamps, 0);
        assert_eq!(router.stats().pending_outbound, 1);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_large_outbound_queue_stays_out_of_ram() {
        use crate::storage::spawn_sqlite_storage_actor;

        let directory = tempfile::tempdir().unwrap();
        let storage =
            spawn_sqlite_storage_actor(directory.path().join("large-outbound.sqlite")).unwrap();
        let mut router = LxmRouter::with_shared_storage_backend(RouterConfig::default(), storage);
        for index in 0..64_u8 {
            let mut message = durable_outbound_message(DeliveryMethod::Direct);
            message.content = format!("outbound-{index}");
            message
                .sign(&rns_crypto::ed25519::Ed25519PrivateKey::generate())
                .unwrap();
            message.next_delivery_attempt = now_f64() + 3_600.0;
            router.send(message);
        }

        assert_eq!(router.stats().pending_outbound, 64);
        assert!(router.pending_outbound.is_empty());
        assert!(router.outbound_summaries(8).len() <= 8);
        assert!(router.process_outbound().is_empty());
        assert!(router.pending_outbound.is_empty());
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_tickets_and_stamp_costs_survive_restart() {
        use crate::storage::spawn_sqlite_storage_actor;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("delivery-state.sqlite");
        let destination = [0xD1; 16];
        let token = [0xE1; 16];
        {
            let storage = spawn_sqlite_storage_actor(path.clone()).unwrap();
            let mut router =
                LxmRouter::with_shared_storage_backend(RouterConfig::default(), storage);
            router.remember_ticket(destination, token, now_f64() + 3_600.0);
            router.set_stamp_cost(destination, 12);
            assert!(router.ticket_store.all().is_empty());
            assert!(router.outbound_stamp_costs.is_empty());
        }

        let storage = spawn_sqlite_storage_actor(path).unwrap();
        let router = LxmRouter::with_shared_storage_backend(RouterConfig::default(), storage);
        assert_eq!(router.get_outbound_ticket(&destination), Some(token));
        assert_eq!(router.get_stamp_cost(&destination), Some(12));
    }

    fn direct_policy_message() -> LxMessage {
        let mut message = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Direct",
            "hello",
            DeliveryMethod::Direct,
        );
        message.compute_hash().unwrap();
        message
    }

    #[test]
    fn test_router_creation() {
        let router = LxmRouter::new(RouterConfig::default());
        assert!(router.pending_outbound.is_empty());
        assert!(router.peers.is_empty());
        assert!(router.propagation_store.is_empty());
    }

    #[test]
    fn high_level_snapshots_are_owned_and_bounded() {
        let mut router = LxmRouter::new(RouterConfig::default());
        router.allow_control([0x11; 16]);
        router.add_static_peer([0x22; 16]);
        router.send(direct_policy_message());

        let mut allowed = router.control_allowed_identities(10);
        let mut peers = router.peer_hashes(10);
        let outbound = router.outbound_summaries(1);

        allowed.clear();
        peers.clear();
        assert!(router.is_control_allowed(&[0x11; 16]));
        assert!(router.has_peer(&[0x22; 16]));
        assert_eq!(outbound.len(), 1);
        assert_eq!(outbound[0].destination_hash, [0xAA; 16]);
        assert!(router.outbound_summaries(0).is_empty());
    }

    #[test]
    fn peer_sync_is_updated_through_router_command() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let peer_hash = [0x33; 16];
        assert!(!router.request_peer_sync(&peer_hash));
        router.add_static_peer(peer_hash);
        assert!(router.request_peer_sync(&peer_hash));
    }

    #[test]
    fn propagation_metadata_queries_are_owned_and_paginated() {
        let mut storage = MemoryStorage::new();
        for byte in [3_u8, 1, 2] {
            storage
                .insert_message(&crate::storage::StoredMessage::new(
                    [byte; 32],
                    [byte + 10; 32],
                    [byte + 20; 16],
                    i64::from(byte),
                    u16::from(byte),
                    vec![byte],
                    false,
                ))
                .unwrap();
        }
        let router = LxmRouter::with_storage_backend(RouterConfig::default(), Box::new(storage));

        let first = router.propagation_metadata_page(None, 2).unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].transient_id, [1; 32]);
        assert_eq!(first[1].transient_id, [2; 32]);

        let second = router
            .propagation_metadata_page(Some(&first[1].transient_id), 2)
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].transient_id, [3; 32]);
        assert_eq!(
            router
                .propagation_metadata(&[3; 32])
                .unwrap()
                .unwrap()
                .stamp_value,
            3
        );
        assert!(
            router
                .propagation_metadata_page(None, 0)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn transient_ids_use_injected_memory_storage() {
        let mut router = LxmRouter::with_storage_backend(
            RouterConfig::default(),
            Box::new(MemoryStorage::new()),
        );
        let transient_id = [0x55; 32];
        assert!(!router.is_locally_delivered(&transient_id).unwrap());
        router.mark_locally_delivered(transient_id).unwrap();
        router.mark_locally_processed(transient_id).unwrap();
        assert!(router.is_locally_delivered(&transient_id).unwrap());
        assert!(router.is_locally_processed(&transient_id).unwrap());
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn transient_ids_survive_router_restart_with_sqlite() {
        use crate::storage::SqliteStorage;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("router.sqlite");
        let transient_id = [0x66; 32];
        {
            let storage = SqliteStorage::open(&path).unwrap();
            let mut router =
                LxmRouter::with_storage_backend(RouterConfig::default(), Box::new(storage));
            router.mark_locally_delivered(transient_id).unwrap();
        }
        let storage = SqliteStorage::open(&path).unwrap();
        let mut restarted =
            LxmRouter::with_storage_backend(RouterConfig::default(), Box::new(storage));
        assert!(restarted.is_locally_delivered(&transient_id).unwrap());
    }

    #[test]
    fn run_jobs_tick_gates_on_processing_interval() {
        let mut router = LxmRouter::new(RouterConfig::default());
        router.run_jobs_tick();
        assert_eq!(router.processing_count, 1);
        // Second call inside the PROCESSING_INTERVAL window must not advance
        // the jobloop — embedders may call this at arbitrary loop rates.
        router.run_jobs_tick();
        assert_eq!(router.processing_count, 1);
        // Backdating the gate simulates the interval elapsing.
        router.last_jobs_tick = now_f64() - PROCESSING_INTERVAL as f64;
        router.run_jobs_tick();
        assert_eq!(router.processing_count, 2);
    }

    #[test]
    fn run_jobs_tick_cleans_transient_caches_at_job_interval() {
        let mut storage = MemoryStorage::new();
        storage
            .upsert_transient_id(TransientIdKind::LocallyDelivered, [0xAB; 32], 1)
            .unwrap();
        let mut router =
            LxmRouter::with_storage_backend(RouterConfig::default(), Box::new(storage));

        // Land exactly on the transient-cache job multiple.
        router.processing_count = JOB_TRANSIENT_INTERVAL - 1;
        router.last_jobs_tick = now_f64() - PROCESSING_INTERVAL as f64;
        router.run_jobs_tick();

        assert!(!router.is_locally_delivered(&[0xAB; 32]).unwrap());
    }

    #[test]
    fn test_router_config_defaults() {
        let config = RouterConfig::default();
        assert!(config.autopeer);
        assert_eq!(config.ext.autopeer_maxdepth, 4);
        assert_eq!(config.ext.propagation_cost_min, 13);
        assert_eq!(config.ext.max_peering_cost, 26);
        assert!(config.ext.processing_outbound);
        assert!(config.ext.defer_stamp_generation);
        assert!(config.ext.processing_limit.is_none());
    }

    #[test]
    fn test_direct_delivery_plan_starts_link_with_current_path() {
        let mut msg = direct_policy_message();
        let dest = msg.destination_hash;
        let now = 1000.0;
        let plan = plan_direct_delivery(
            &mut msg,
            DirectDeliveryPlanInput {
                identity_known: true,
                route: Some(DirectRouteSnapshot::new(dest, 4)),
                reusable_link: DirectReusableLinkState::None,
            },
            now,
        );

        assert_eq!(plan, DirectDeliveryPlan::StartNewLink { hops: 4 });
        assert_eq!(msg.delivery_attempts, 1);
        assert_eq!(msg.last_delivery_attempt, now);
        assert_eq!(msg.next_delivery_attempt, now + DELIVERY_RETRY_WAIT as f64);
        assert_eq!(msg.progress, 0.03);
    }

    #[test]
    fn test_direct_delivery_plan_requests_path_for_unknown_identity() {
        let mut msg = direct_policy_message();
        let now = 1000.0;
        let plan = plan_direct_delivery(
            &mut msg,
            DirectDeliveryPlanInput {
                identity_known: false,
                route: None,
                reusable_link: DirectReusableLinkState::None,
            },
            now,
        );

        assert_eq!(
            plan,
            DirectDeliveryPlan::RequestPath {
                drop_existing: false
            }
        );
        assert_eq!(msg.delivery_attempts, 1);
        assert_eq!(msg.last_delivery_attempt, now);
        assert_eq!(msg.next_delivery_attempt, now + PATH_REQUEST_WAIT as f64);
        assert_eq!(msg.progress, 0.01);
    }

    #[test]
    fn test_direct_delivery_plan_reuses_active_link_without_attempt_increment() {
        let mut msg = direct_policy_message();
        let plan = plan_direct_delivery(
            &mut msg,
            DirectDeliveryPlanInput {
                identity_known: true,
                route: None,
                reusable_link: DirectReusableLinkState::Active,
            },
            1000.0,
        );

        assert_eq!(plan, DirectDeliveryPlan::UseReusableLink);
        assert_eq!(msg.delivery_attempts, 0);
        assert_eq!(msg.state, MessageState::Sending);
        assert_eq!(msg.progress, 0.05);
    }

    #[test]
    fn test_direct_delivery_plan_waits_for_pending_link() {
        let mut msg = direct_policy_message();
        let dest = msg.destination_hash;
        let plan = plan_direct_delivery(
            &mut msg,
            DirectDeliveryPlanInput {
                identity_known: true,
                route: Some(DirectRouteSnapshot::new(dest, 2)),
                reusable_link: DirectReusableLinkState::Pending,
            },
            1000.0,
        );

        assert_eq!(plan, DirectDeliveryPlan::WaitForReusableLink);
        assert_eq!(msg.delivery_attempts, 0);
        assert_eq!(msg.next_delivery_attempt, 0.0);
    }

    #[test]
    fn test_direct_delivery_plan_defers_terminal_failure_at_attempt_boundary() {
        let mut msg = direct_policy_message();
        let dest = msg.destination_hash;
        msg.delivery_attempts = MAX_DELIVERY_ATTEMPTS - 1;
        let now = 1000.0;
        let plan = plan_direct_delivery(
            &mut msg,
            DirectDeliveryPlanInput {
                identity_known: true,
                route: Some(DirectRouteSnapshot::new(dest, 1)),
                reusable_link: DirectReusableLinkState::None,
            },
            now,
        );

        assert_eq!(plan, DirectDeliveryPlan::DeferTerminalFailure);
        assert_eq!(msg.delivery_attempts, MAX_DELIVERY_ATTEMPTS);
        assert_eq!(msg.next_delivery_attempt, now + DELIVERY_RETRY_WAIT as f64);
    }

    #[test]
    fn test_direct_delivery_plan_requests_rediscovery_for_closed_link() {
        let mut msg = direct_policy_message();
        let dest = msg.destination_hash;
        let now = 1000.0;
        let plan = plan_direct_delivery(
            &mut msg,
            DirectDeliveryPlanInput {
                identity_known: true,
                route: Some(DirectRouteSnapshot::new(dest, 3)),
                reusable_link: DirectReusableLinkState::Closed { activated: false },
            },
            now,
        );

        assert_eq!(
            plan,
            DirectDeliveryPlan::RequestPath {
                drop_existing: true
            }
        );
        assert_eq!(msg.delivery_attempts, 0);
        assert_eq!(msg.next_delivery_attempt, now + PATH_REQUEST_WAIT as f64);
        assert_eq!(msg.progress, 0.01);
    }

    #[test]
    fn test_direct_delivery_plan_fails_above_max_attempts() {
        let mut msg = direct_policy_message();
        let dest = msg.destination_hash;
        msg.delivery_attempts = MAX_DELIVERY_ATTEMPTS + 1;
        let plan = plan_direct_delivery(
            &mut msg,
            DirectDeliveryPlanInput {
                identity_known: true,
                route: Some(DirectRouteSnapshot::new(dest, 1)),
                reusable_link: DirectReusableLinkState::None,
            },
            1000.0,
        );

        assert_eq!(plan, DirectDeliveryPlan::Fail);
        assert_eq!(msg.state, MessageState::Failed);
    }

    #[test]
    fn test_process_outbound_with_direct_keeps_message_pending() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let msg = direct_policy_message();
        let msg_hash = msg.hash;
        let dest = msg.destination_hash;
        router.send(msg);

        let actions =
            router.process_outbound_with_direct(|message, _now| DirectDeliveryPlanInput {
                identity_known: true,
                route: Some(DirectRouteSnapshot::new(message.destination_hash, 4)),
                reusable_link: DirectReusableLinkState::None,
            });

        assert_eq!(router.pending_outbound.len(), 1);
        let pending = &router.pending_outbound[0];
        assert_eq!(pending.hash, msg_hash);
        assert_eq!(pending.state, MessageState::Sending);
        assert_eq!(pending.delivery_attempts, 1);
        assert_eq!(pending.progress, 0.03);
        match actions.as_slice() {
            [
                OutboundAction::PlanDirect {
                    message,
                    dest_hash,
                    plan,
                },
            ] => {
                assert_eq!(*dest_hash, dest);
                assert_eq!(message.hash, msg_hash);
                assert_eq!(*plan, DirectDeliveryPlan::StartNewLink { hops: 4 });
            }
            other => panic!("expected PlanDirect StartNewLink, got {other:?}"),
        }
    }

    #[test]
    fn test_process_outbound_with_direct_polls_pending_link_despite_retry_deadline() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let mut msg = direct_policy_message();
        msg.next_delivery_attempt = now_f64() + 3600.0;
        let msg_hash = msg.hash;
        router.send(msg);

        let actions =
            router.process_outbound_with_direct(|_message, _now| DirectDeliveryPlanInput {
                identity_known: true,
                route: None,
                reusable_link: DirectReusableLinkState::Pending,
            });

        assert_eq!(router.pending_outbound.len(), 1);
        assert_eq!(router.pending_outbound[0].hash, msg_hash);
        assert_eq!(router.pending_outbound[0].delivery_attempts, 0);
        match actions.as_slice() {
            [OutboundAction::PlanDirect { plan, .. }] => {
                assert_eq!(*plan, DirectDeliveryPlan::WaitForReusableLink);
            }
            other => panic!("expected PlanDirect WaitForReusableLink, got {other:?}"),
        }
    }

    #[test]
    fn test_outbound_terminal_helpers_update_pending_queue() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let first = direct_policy_message();
        let first_hash = first.hash.expect("first hash");
        router.send(first);

        assert!(router.defer_outbound_for_path_request(&first_hash, 1000.0));
        assert_eq!(
            router.pending_outbound[0].next_delivery_attempt,
            1000.0 + PATH_REQUEST_WAIT as f64
        );
        assert_eq!(router.pending_outbound[0].progress, 0.01);

        assert!(router.mark_outbound_delivered(&first_hash));
        assert!(router.pending_outbound.is_empty());

        let second = direct_policy_message();
        let second_hash = second.hash.expect("second hash");
        router.send(second);
        assert!(router.mark_outbound_failed(&second_hash));
        assert!(router.pending_outbound.is_empty());
    }

    #[test]
    fn test_allow_disallow() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let hash = [0xAA; 16];

        // Empty allow-list means all allowed.
        assert!(router.is_allowed(&hash));

        router.allow(hash);
        assert!(router.is_allowed(&hash));
        assert!(!router.is_allowed(&[0xBB; 16]));

        router.disallow(&hash);
        assert!(router.is_allowed(&hash));
    }

    #[test]
    fn test_block_unblock() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let hash = [0xAA; 16];

        assert!(router.is_allowed(&hash));

        router.block(hash);
        assert!(!router.is_allowed(&hash));

        router.unblock(&hash);
        assert!(router.is_allowed(&hash));
    }

    #[test]
    fn test_prioritise_unprioritise() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let hash = [0xAA; 16];

        router.prioritise(hash, 5);
        assert_eq!(router.prioritized.get(&hash), Some(&5));

        router.unprioritise(&hash);
        assert!(!router.prioritized.contains_key(&hash));
    }

    #[test]
    fn test_add_peer() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let peer = LxmPeer::new([0xAA; 16]);
        assert!(router.add_peer(peer));
        assert_eq!(router.peers.len(), 1);
    }

    #[test]
    fn test_max_peers() {
        let config = RouterConfig {
            max_peers: 2,
            ..Default::default()
        };
        let mut router = LxmRouter::new(config);

        assert!(router.add_peer(LxmPeer::new([0x01; 16])));
        assert!(router.add_peer(LxmPeer::new([0x02; 16])));
        assert!(!router.add_peer(LxmPeer::new([0x03; 16])));
    }

    #[test]
    fn test_stamp_cost_cache() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let dest = [0xAA; 16];

        assert!(router.get_stamp_cost(&dest).is_none());

        router.set_stamp_cost(dest, 12);
        assert_eq!(router.get_stamp_cost(&dest), Some(12));
    }

    #[test]
    fn test_send_uses_outbound_ticket_stamp_immediately() {
        let key = rns_crypto::ed25519::Ed25519PrivateKey::generate();
        let mut router = LxmRouter::new(RouterConfig::default());
        let dest = [0xAA; 16];
        router.remember_ticket(dest, [0x42; 16], now_f64() + 60.0);
        router.set_stamp_cost(dest, 16);

        let mut msg = LxMessage::new(dest, [0xBB; 16], "ticket", "stamp", DeliveryMethod::Direct);
        msg.sign(&key).unwrap();
        router.send(msg);

        assert!(router.pending_deferred_stamps.is_empty());
        assert_eq!(router.pending_outbound.len(), 1);
        let queued = &router.pending_outbound[0];
        assert_eq!(queued.stamp.as_ref().map(Vec::len), Some(TICKET_LENGTH));
        assert_eq!(queued.stamp_value, Some(COST_TICKET));
    }

    #[tokio::test]
    async fn test_deferred_stamp_queue_completes_before_outbound_processing() {
        let key = rns_crypto::ed25519::Ed25519PrivateKey::generate();
        let mut router = LxmRouter::new(RouterConfig::default());
        let dest = [0xAA; 16];
        router.set_stamp_cost(dest, 1);

        let mut msg = LxMessage::new(dest, [0xBB; 16], "defer", "stamp", DeliveryMethod::Direct);
        msg.sign(&key).unwrap();
        let message_id = msg.message_id.unwrap();
        router.send(msg);

        assert!(router.pending_outbound.is_empty());
        assert!(router.pending_deferred_stamps.contains_key(&message_id));

        for _ in 0..100 {
            router.process_deferred_stamps();
            if router.pending_outbound.len() == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert!(router.pending_deferred_stamps.is_empty());
        assert_eq!(router.pending_outbound.len(), 1);
        let queued = &router.pending_outbound[0];
        assert_eq!(queued.stamp.as_ref().map(Vec::len), Some(32));
        assert!(queued.stamp_value.unwrap_or(0) >= 1);
    }

    /// Production shape: the manager tick runs on `spawn_blocking`, where
    /// `Handle::try_current()` fails. The router's stored handle must still
    /// spawn the PoW worker instead of grinding inline under the caller.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deferred_stamps_spawn_from_blocking_thread_via_stored_handle() {
        let key = rns_crypto::ed25519::Ed25519PrivateKey::generate();
        // Constructed inside the runtime: handle captured.
        let mut router = LxmRouter::new(RouterConfig::default());
        assert!(
            router.runtime_handle.is_some(),
            "handle captured at construction"
        );
        let dest = [0xAA; 16];
        router.set_stamp_cost(dest, 1);

        let mut msg = LxMessage::new(
            dest,
            [0xBB; 16],
            "defer",
            "blocking",
            DeliveryMethod::Direct,
        );
        msg.sign(&key).unwrap();
        let message_id = msg.message_id.unwrap();
        router.send(msg);
        assert!(router.pending_deferred_stamps.contains_key(&message_id));

        // Drive entirely from a blocking thread (no implicit runtime).
        let mut router = tokio::task::spawn_blocking(move || {
            router.process_deferred_stamps();
            assert!(
                router.active_deferred_stamp.is_some(),
                "must spawn a worker, not grind inline"
            );
            router
        })
        .await
        .unwrap();

        for _ in 0..200 {
            router.process_deferred_stamps();
            if router.pending_outbound.len() == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(router.pending_outbound.len(), 1);
        assert_eq!(
            router.pending_outbound[0].stamp.as_ref().map(Vec::len),
            Some(32)
        );
    }

    /// Delivery-time deferral: a message that reaches the outbound executor
    /// without a stamp goes back through the deferred queue instead of
    /// blocking the caller.
    #[tokio::test]
    async fn defer_stamp_queues_message_with_id() {
        let key = rns_crypto::ed25519::Ed25519PrivateKey::generate();
        let mut router = LxmRouter::new(RouterConfig::default());
        let mut msg = LxMessage::new([0xAA; 16], [0xBB; 16], "d", "s", DeliveryMethod::Direct);
        msg.sign(&key).unwrap();
        msg.stamp_cost = Some(1);
        let id = msg.message_id.unwrap();

        assert!(router.defer_stamp(msg).is_none());
        assert!(router.pending_deferred_stamps.contains_key(&id));
    }

    #[tokio::test]
    async fn test_cancel_outbound_cancels_deferred_stamp_job() {
        let key = rns_crypto::ed25519::Ed25519PrivateKey::generate();
        let mut router = LxmRouter::new(RouterConfig::default());
        let dest = [0xAA; 16];
        router.set_stamp_cost(dest, 8);

        let mut msg = LxMessage::new(dest, [0xBB; 16], "cancel", "stamp", DeliveryMethod::Direct);
        msg.sign(&key).unwrap();
        let message_id = msg.message_id.unwrap();
        router.send(msg);
        router.process_deferred_stamps();

        assert!(router.active_deferred_stamp.is_some());
        assert!(router.cancel_outbound(&message_id));
        assert!(router.pending_deferred_stamps.is_empty());
        assert!(router.active_deferred_stamp.is_none());
    }

    #[test]
    fn test_authentication_accessors() {
        let mut router = LxmRouter::new(RouterConfig::default());
        assert!(!router.requires_authentication());
        router.set_authentication(true);
        assert!(router.requires_authentication());
    }

    #[test]
    fn test_retain_node_lxms_accessors() {
        let mut router = LxmRouter::new(RouterConfig::default());
        assert!(!router.retain_node_lxms());
        router.set_retain_node_lxms(true);
        assert!(router.retain_node_lxms());
    }

    #[test]
    fn test_message_storage_limit_accessors() {
        let mut router = LxmRouter::new(RouterConfig::default());
        assert!(router.message_storage_limit().is_none());
        assert_eq!(router.message_storage_size(), 0);

        router.set_message_storage_limit(Some(1024 * 1024));
        assert_eq!(router.message_storage_limit(), Some(1024 * 1024));
    }

    #[test]
    fn test_ticket_api() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let dest = [0xAA; 16];

        assert!(router.get_outbound_ticket(&dest).is_none());
        assert!(router.get_outbound_ticket_expiry(&dest).is_none());
        assert!(router.get_inbound_tickets().is_empty());

        let token = router.generate_ticket(dest, None);
        assert_eq!(router.get_outbound_ticket(&dest), Some(token));
        assert!(router.get_outbound_ticket_expiry(&dest).unwrap() > now_f64());
        assert_eq!(router.get_inbound_tickets().len(), 1);

        // remember_ticket adds another entry for the same dest.
        router.remember_ticket(dest, [0x55; 16], now_f64() + 1000.0);
        assert_eq!(router.get_inbound_tickets().len(), 2);
    }

    #[test]
    fn test_validate_stamp_checks_all_tickets() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let dest = [0xAA; 16];
        let expires = now_f64() + 1000.0;
        router.remember_ticket(dest, [0x01; 16], expires);
        router.remember_ticket(dest, [0x02; 16], expires);

        // Stamp derived from the SECOND ticket must still validate.
        let message_id = [0x33u8; 32];
        let mut material = Vec::with_capacity(16 + 32);
        material.extend_from_slice(&[0x02; 16]);
        material.extend_from_slice(&message_id);
        let stamp = rns_crypto::sha::truncated_hash(&material);

        assert!(router.validate_stamp_with_tickets(&message_id, stamp.as_ref(), 16, &dest));
    }

    #[test]
    fn test_cancel_outbound() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let mut msg = LxMessage::new([0xAA; 16], [0xBB; 16], "t", "c", DeliveryMethod::Direct);
        msg.state = MessageState::Outbound;
        let hash = [0x11u8; 32];
        msg.hash = Some(hash);
        router.pending_outbound.push(msg);

        assert!(router.cancel_outbound(&hash));
        assert!(router.pending_outbound.is_empty());
        assert!(!router.cancel_outbound(&hash));
    }

    #[test]
    fn test_get_outbound_progress() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let mut msg = LxMessage::new([0xAA; 16], [0xBB; 16], "t", "c", DeliveryMethod::Direct);
        msg.progress = 0.42;
        let hash = [0x22u8; 32];
        msg.hash = Some(hash);
        router.pending_outbound.push(msg);

        assert_eq!(router.get_outbound_progress(&hash), Some(0.42));
        assert_eq!(router.get_outbound_progress(&[0u8; 32]), None);
    }

    #[test]
    fn test_register_delivery_callback() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let mut router = LxmRouter::new(RouterConfig::default());
        let fired = Arc::new(AtomicBool::new(false));
        let fired_clone = fired.clone();
        router.register_delivery_callback(move |_| {
            fired_clone.store(true, Ordering::Relaxed);
        });

        let msg = LxMessage::new([0xAA; 16], [0xBB; 16], "t", "c", DeliveryMethod::Direct);
        (router.delivery_callback.as_ref().unwrap())(&msg);
        assert!(fired.load(Ordering::Relaxed));
    }

    #[test]
    fn test_ingest_lxm_uri() {
        use rns_crypto::ed25519::Ed25519PrivateKey;

        let mut router = LxmRouter::new(RouterConfig::default());
        let key = Ed25519PrivateKey::generate();
        let mut msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "paper",
            "hello",
            DeliveryMethod::Paper,
        );
        msg.sign(&key).unwrap();
        let uri = msg
            .to_paper_uri(|plaintext| Ok(plaintext.to_vec()))
            .unwrap();

        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        let fired = Arc::new(AtomicBool::new(false));
        let fired_clone = fired.clone();
        router.register_delivery_callback(move |_| {
            fired_clone.store(true, Ordering::Relaxed);
        });

        let decoded = router
            .ingest_lxm_uri(&uri, |ciphertext| Ok(ciphertext.to_vec()))
            .unwrap();
        assert_eq!(decoded.title, "paper");
        assert!(fired.load(Ordering::Relaxed));
    }

    #[test]
    fn test_sync_peers_picks_due_peers() {
        let mut router = LxmRouter::new(RouterConfig::default());

        let mut peer_due = LxmPeer::new([0x01; 16]);
        peer_due.add_unhandled_message();
        router.add_peer(peer_due);

        let peer_idle_no_msgs = LxmPeer::new([0x02; 16]);
        router.add_peer(peer_idle_no_msgs);

        let mut peer_in_flight = LxmPeer::new([0x03; 16]);
        peer_in_flight.add_unhandled_message();
        peer_in_flight.begin_sync();
        router.add_peer(peer_in_flight);

        let due = router.sync_peers();
        assert_eq!(due, vec![[0x01; 16]]);

        // Subsequent call returns empty — the due peer is now LinkEstablishing.
        assert!(router.sync_peers().is_empty());
    }

    #[test]
    fn test_validate_stamp() {
        let router = LxmRouter::new(RouterConfig::default());
        let msg_id = rns_crypto::sha::sha256(b"test message");
        let stamp =
            stamper::generate_stamp_limited(&msg_id, 4, STAMP_WORKBLOCK_EXPAND_ROUNDS, 1_000_000);
        if let Some(stamp) = stamp {
            assert!(router.validate_stamp(&msg_id, &stamp, 4));
        }
    }

    #[test]
    fn test_send_message() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Test",
            "Content",
            DeliveryMethod::Direct,
        );
        router.send(msg);
        assert_eq!(router.pending_outbound.len(), 1);
        assert_eq!(router.pending_outbound[0].state, MessageState::Outbound);
    }

    #[test]
    fn test_try_send_propagated_without_node_fails_immediately() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Propagated",
            "Content",
            DeliveryMethod::Propagated,
        );

        let err = router.try_send(msg).unwrap_err();
        assert!(matches!(err, SendError::MissingOutboundPropagationNode(_)));
        assert_eq!(err.message().state, MessageState::Failed);
        assert!(router.pending_outbound.is_empty());
    }

    /// Python fails a message only when delivery_attempts > MAX_DELIVERY_ATTEMPTS
    /// (outer guard is `<= MAX`, LXMRouter.py:2597/2671). Pin that boundary.
    #[test]
    fn test_process_outbound_fails_only_above_max_attempts() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let mut msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Test",
            "Content",
            DeliveryMethod::Direct,
        );
        msg.delivery_attempts = MAX_DELIVERY_ATTEMPTS + 1;
        router.send(msg);

        let actions = router.process_outbound();
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], OutboundAction::Failed(_)));
        assert!(router.pending_outbound.is_empty());
    }

    /// At exactly MAX the message is still attempted (dispatched), not failed.
    #[test]
    fn test_process_outbound_at_max_attempts_not_failed() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let mut msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Test",
            "Content",
            DeliveryMethod::Direct,
        );
        msg.delivery_attempts = MAX_DELIVERY_ATTEMPTS;
        router.send(msg);

        let actions = router.process_outbound();
        assert_eq!(actions.len(), 1);
        assert!(
            !matches!(actions[0], OutboundAction::Failed(_)),
            "at exactly MAX the message is still attempted, not failed"
        );
    }

    #[test]
    fn test_stats() {
        let router = LxmRouter::new(RouterConfig::default());
        let stats = router.stats();
        assert_eq!(stats.pending_outbound, 0);
        assert_eq!(stats.peers, 0);
        assert_eq!(stats.propagation_entries, 0);
    }

    #[test]
    fn test_allow_control() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let hash = [0xCC; 16];

        router.allow_control(hash);
        assert!(router.is_control_allowed(&hash));
        assert!(!router.is_control_allowed(&[0xDD; 16]));

        router.disallow_control(&hash);
        assert!(!router.is_control_allowed(&hash));
    }

    #[test]
    fn test_set_transport() {
        let mut router = LxmRouter::new(RouterConfig::default());
        assert!(!router.has_transport());
        assert!(router.transport_tx.is_none());

        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        router.set_transport(tx);
        assert!(router.has_transport());
        assert!(router.transport_tx.is_some());
    }

    #[test]
    fn test_process_outbound_direct_delivery() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Direct",
            "Content",
            DeliveryMethod::Direct,
        );
        router.send(msg);

        let actions = router.process_outbound();
        let has_direct = actions
            .iter()
            .any(|a| matches!(a, OutboundAction::DeliverDirect { .. }));
        assert!(has_direct);
    }

    #[test]
    fn test_process_outbound_propagated_delivery() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let peer_hash = [0x11; 16];
        let peer = crate::peer::LxmPeer::new(peer_hash);
        router.add_peer(peer);
        router.set_outbound_propagation_node(Some(peer_hash));

        let msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Propagated",
            "Content",
            DeliveryMethod::Propagated,
        );
        router.send(msg);

        let actions = router.process_outbound();
        let has_propagated = actions
            .iter()
            .any(|a| matches!(a, OutboundAction::DeliverPropagated { .. }));
        assert!(has_propagated);
    }

    #[test]
    fn test_process_outbound_opportunistic_delivery() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Opportunistic",
            "Content",
            DeliveryMethod::Opportunistic,
        );
        router.send(msg);

        let actions = router.process_outbound();
        let has_opportunistic = actions
            .iter()
            .any(|a| matches!(a, OutboundAction::DeliverOpportunistic { .. }));
        assert!(has_opportunistic);
    }

    #[test]
    fn test_process_outbound_propagated_no_node() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Propagated",
            "Content",
            DeliveryMethod::Propagated,
        );
        router.send(msg);

        assert!(
            router.pending_outbound.is_empty(),
            "propagated messages without an outbound node fail at queue time"
        );
    }

    #[test]
    fn test_delivery_announce_trigger_clears_direct_retry_backoff() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let dest = [0xAA; 16];
        let mut msg = LxMessage::new(
            dest,
            [0xBB; 16],
            "Direct",
            "Content",
            DeliveryMethod::Direct,
        );
        msg.delivery_attempts = 1;
        msg.last_delivery_attempt = now_f64();
        msg.next_delivery_attempt = now_f64() + PATH_REQUEST_WAIT as f64;
        router.send(msg);

        assert!(router.process_outbound().is_empty());
        assert_eq!(router.trigger_outbound_for_delivery_announce(dest), 1);
        assert!(matches!(
            router.process_outbound().as_slice(),
            [OutboundAction::DeliverDirect { .. }]
        ));
    }

    #[test]
    fn test_delivery_announce_trigger_clears_propagated_recipient_backoff() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let dest = [0xAA; 16];
        let node = [0xCC; 16];
        router.set_outbound_propagation_node(Some(node));
        let mut msg = LxMessage::new(
            dest,
            [0xBB; 16],
            "Propagated",
            "Content",
            DeliveryMethod::Propagated,
        );
        msg.delivery_attempts = 1;
        msg.last_delivery_attempt = now_f64();
        msg.next_delivery_attempt = now_f64() + PATH_REQUEST_WAIT as f64;
        router.send(msg);

        assert!(router.process_outbound().is_empty());
        assert_eq!(router.trigger_outbound_for_delivery_announce(dest), 1);
        assert!(matches!(
            router.process_outbound().as_slice(),
            [OutboundAction::DeliverPropagated { .. }]
        ));
    }

    #[test]
    fn test_propagation_node_announce_trigger_clears_propagated_retry_backoff() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let node = [0xCC; 16];
        router.set_outbound_propagation_node(Some(node));
        let mut msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Propagated",
            "Content",
            DeliveryMethod::Propagated,
        );
        msg.delivery_attempts = 1;
        msg.last_delivery_attempt = now_f64();
        msg.next_delivery_attempt = now_f64() + PATH_REQUEST_WAIT as f64;
        router.send(msg);

        assert!(router.process_outbound().is_empty());

        let pn_data = crate::handlers::get_propagation_node_app_data(
            &crate::handlers::PropagationNodeAnnounceData::new(true, 256, 10240, 16, 3, 18),
        );
        assert_eq!(
            router.trigger_outbound_for_propagation_node_announce(node, &pn_data),
            1
        );
        assert!(matches!(
            router.process_outbound().as_slice(),
            [OutboundAction::DeliverPropagated { .. }]
        ));
    }

    #[test]
    fn test_propagation_node_announce_trigger_requires_configured_valid_node() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let node = [0xCC; 16];
        router.set_outbound_propagation_node(Some(node));
        router.send(LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Propagated",
            "Content",
            DeliveryMethod::Propagated,
        ));

        assert_eq!(
            router.trigger_outbound_for_propagation_node_announce([0xDD; 16], b"not-msgpack"),
            0
        );
        assert_eq!(
            router.trigger_outbound_for_propagation_node_announce(node, b"not-msgpack"),
            0
        );
    }

    /// A queued message older than `MESSAGE_EXPIRY` must be flushed as
    /// `Expired` on the next `process_outbound`, marked Failed, and not
    /// held indefinitely. Mirrors Python LXMRouter.process_outbound where
    /// the age check runs before any delivery attempt.
    #[test]
    fn test_process_outbound_expired_message_marked_failed() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let mut msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Stale",
            "Content",
            DeliveryMethod::Direct,
        );
        // Anchor the timestamp comfortably past the expiry window.
        msg.timestamp = now_f64() - (MESSAGE_EXPIRY as f64) - 60.0;
        router.send(msg);

        let actions = router.process_outbound();
        assert_eq!(actions.len(), 1);
        assert!(
            matches!(&actions[0], OutboundAction::Expired(m) if m.state == MessageState::Failed),
            "expired message surfaces as Expired with state=Failed, got {:?}",
            actions[0]
        );
        assert!(
            router.pending_outbound.is_empty(),
            "expired message removed from queue"
        );
    }

    /// A message that has attempted delivery within the last
    /// `DELIVERY_RETRY_WAIT` seconds must be skipped by `process_outbound`
    /// rather than immediately retried. Prevents tight-loop reattempt
    /// storms when a transport has a transient failure.
    #[test]
    fn test_process_outbound_retry_backoff_defers_within_window() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let mut msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Backoff",
            "Content",
            DeliveryMethod::Direct,
        );
        // Simulate one failed attempt very recently.
        msg.delivery_attempts = 1;
        msg.last_delivery_attempt = now_f64() - 1.0; // 1 s ago, inside the 10 s window.
        router.send(msg);

        let actions = router.process_outbound();
        assert!(
            actions.is_empty(),
            "inside retry-wait window: no action emitted, got {:?}",
            actions
        );
        assert_eq!(
            router.pending_outbound.len(),
            1,
            "message stays queued for the next tick"
        );
    }

    /// Waiting for route or metadata preconditions is not itself a failed
    /// delivery attempt, but it still needs the same retry backoff to avoid
    /// tight request-path loops.
    #[test]
    fn test_process_outbound_retry_backoff_uses_last_attempt_timestamp() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let mut msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Backoff",
            "Content",
            DeliveryMethod::Propagated,
        );
        msg.last_delivery_attempt = now_f64() - 1.0;
        router.set_outbound_propagation_node(Some([0xCC; 16]));
        router.send(msg);

        let actions = router.process_outbound();
        assert!(
            actions.is_empty(),
            "metadata waits should stay queued inside retry-wait window"
        );
        assert_eq!(router.pending_outbound.len(), 1);
        assert_eq!(router.pending_outbound[0].delivery_attempts, 0);
    }

    /// The state machine contract: a Direct message picked up by
    /// `process_outbound` must be emitted as `DeliverDirect` with the
    /// message state transitioned to `Sending` before it leaves the queue.
    /// Complements `test_process_outbound_direct_delivery`, which only
    /// covers the action variant without asserting on state.
    #[test]
    fn test_process_outbound_direct_transitions_to_sending() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Direct",
            "Content",
            DeliveryMethod::Direct,
        );
        router.send(msg);

        let actions = router.process_outbound();
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OutboundAction::DeliverDirect { message, .. } => {
                assert_eq!(
                    message.state,
                    MessageState::Sending,
                    "Direct message enters Sending on dequeue"
                );
            }
            other => panic!("expected DeliverDirect, got {:?}", other),
        }
    }

    #[test]
    fn test_tick_sends_to_transport() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        router.set_transport(tx);

        let signing_key = rns_crypto::ed25519::Ed25519PrivateKey::generate();
        let mut msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Tick Test",
            "Content",
            DeliveryMethod::Opportunistic,
        );
        msg.sign(&signing_key).unwrap();
        router.send(msg);

        router.tick_with_encryptor(|_dest, plaintext| Ok(plaintext.to_vec()));

        let received = rx.try_recv();
        assert!(received.is_ok(), "expected outbound packet from tick()");
    }

    #[test]
    fn test_execute_actions_no_transport() {
        let mut router = LxmRouter::new(RouterConfig::default());
        // No transport set — execute_actions must be a no-op.
        let actions = vec![OutboundAction::DeliverDirect {
            message: LxMessage::new([0; 16], [0; 16], "t", "c", DeliveryMethod::Direct),
            dest_hash: [0; 16],
        }];
        router.execute_actions(actions);
    }

    #[test]
    fn test_execute_actions_only_sends_opportunistic_packet_payload_shape() {
        let key = rns_crypto::ed25519::Ed25519PrivateKey::generate();
        let dest_hash = [0xAA; 16];
        let src_hash = [0xBB; 16];

        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let mut router = LxmRouter::new(RouterConfig::default());
        router.set_transport(tx);

        let mut direct = LxMessage::new(dest_hash, src_hash, "Direct", "d", DeliveryMethod::Direct);
        direct.sign(&key).unwrap();

        let mut opportunistic = LxMessage::new(
            dest_hash,
            src_hash,
            "Opp",
            "o",
            DeliveryMethod::Opportunistic,
        );
        opportunistic.sign(&key).unwrap();
        let opportunistic_packed = opportunistic.pack().unwrap();

        router.execute_actions_with_encryptor(
            vec![
                OutboundAction::DeliverDirect {
                    message: direct,
                    dest_hash,
                },
                OutboundAction::DeliverOpportunistic {
                    message: opportunistic,
                    dest_hash,
                },
            ],
            |_dest, plaintext| {
                let mut out = vec![0xEE];
                out.extend_from_slice(plaintext);
                Ok(out)
            },
        );

        let opportunistic_raw = match rx.try_recv().expect("opportunistic outbound request") {
            rns_transport::messages::TransportMessage::Outbound(req) => req.raw,
            other => panic!("expected outbound request, got {other:?}"),
        };
        let (_, opportunistic_data_offset) =
            rns_wire::header::PacketHeader::unpack(&opportunistic_raw).unwrap();
        assert_eq!(
            &opportunistic_raw[opportunistic_data_offset..],
            [&[0xEE], &opportunistic_packed[DESTINATION_LENGTH..]].concat(),
            "Opportunistic delivery encrypts the LXMF tail after the destination hash"
        );
        assert!(
            rx.try_recv().is_err(),
            "Direct actions require LinkDeliveryManager and must not be sent as destination packets"
        );
    }

    #[test]
    fn test_execute_actions_without_encryptor_does_not_send_opportunistic_plaintext() {
        let key = rns_crypto::ed25519::Ed25519PrivateKey::generate();
        let dest_hash = [0xAA; 16];
        let src_hash = [0xBB; 16];

        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let mut router = LxmRouter::new(RouterConfig::default());
        router.set_transport(tx);

        let mut direct = LxMessage::new(dest_hash, src_hash, "Direct", "d", DeliveryMethod::Direct);
        direct.sign(&key).unwrap();

        let mut opportunistic = LxMessage::new(
            dest_hash,
            src_hash,
            "Opp",
            "o",
            DeliveryMethod::Opportunistic,
        );
        opportunistic.sign(&key).unwrap();

        router.execute_actions(vec![
            OutboundAction::DeliverDirect {
                message: direct,
                dest_hash,
            },
            OutboundAction::DeliverOpportunistic {
                message: opportunistic,
                dest_hash,
            },
        ]);

        assert!(
            rx.try_recv().is_err(),
            "execute_actions without an encryptor must not send raw opportunistic payloads"
        );
    }

    #[test]
    fn test_ignore_destination() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let dest = [0xAA; 16];

        router.ignore_destination(dest);
        assert!(router.ignored.contains(&dest));
        assert!(router.propagation_store.is_destination_ignored(&dest));

        router.unignore_destination(&dest);
        assert!(!router.ignored.contains(&dest));
        assert!(!router.propagation_store.is_destination_ignored(&dest));
    }

    #[test]
    fn test_autopeer() {
        let mut router = LxmRouter::new(RouterConfig::default());
        assert!(router.autopeer(AutopeerCandidate {
            destination_hash: [0xAA; 16],
            timebase: 1000.0,
            transfer_limit: Some(256.0),
            sync_limit: Some(10240.0),
            stamp_cost: Some(16),
            stamp_flexibility: Some(3),
            peering_cost: Some(18),
            hops: Some(2),
        }));
        assert_eq!(router.peers.len(), 1);

        assert!(!router.autopeer(AutopeerCandidate {
            destination_hash: [0xAA; 16],
            timebase: 1000.0,
            transfer_limit: None,
            sync_limit: None,
            stamp_cost: None,
            stamp_flexibility: None,
            peering_cost: None,
            hops: None,
        }));
        assert!(!router.autopeer(AutopeerCandidate {
            destination_hash: [0xBB; 16],
            timebase: 1000.0,
            transfer_limit: None,
            sync_limit: None,
            stamp_cost: None,
            stamp_flexibility: None,
            peering_cost: None,
            hops: Some(10),
        }));
    }

    /// T0-4: peering cost above `max_peering_cost` refuses peering, and an
    /// existing peering breaks when the peer raises its cost beyond the cap
    /// (Python LXMRouter.py:1896-1901).
    #[test]
    fn test_autopeer_enforces_max_peering_cost() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let max = router.config.ext.max_peering_cost;

        let candidate = |cost: Option<u8>| AutopeerCandidate {
            destination_hash: [0xAA; 16],
            timebase: 1000.0,
            transfer_limit: Some(256.0),
            sync_limit: None,
            stamp_cost: Some(16),
            stamp_flexibility: Some(3),
            peering_cost: cost,
            hops: Some(2),
        };

        // Over-cost announce: refused outright.
        assert!(!router.autopeer(candidate(Some(max + 1))));
        assert!(router.peers.is_empty());

        // At-cost announce: accepted.
        assert!(router.autopeer(candidate(Some(max))));
        assert_eq!(router.peers.len(), 1);

        // Existing peer raising its cost beyond the cap: peering breaks.
        assert!(!router.autopeer(candidate(Some(max + 1))));
        assert!(router.peers.is_empty(), "over-cost announce must unpeer");
    }

    #[test]
    fn test_autopeer_respects_configured_maxdepth() {
        let mut router = LxmRouter::new(RouterConfig {
            ext: RouterConfigExt {
                autopeer_maxdepth: 1,
                ..Default::default()
            },
            ..Default::default()
        });

        assert!(!router.autopeer(AutopeerCandidate {
            destination_hash: [0xAA; 16],
            timebase: 1000.0,
            transfer_limit: None,
            sync_limit: None,
            stamp_cost: None,
            stamp_flexibility: None,
            peering_cost: None,
            hops: Some(2),
        }));
        assert!(router.autopeer(AutopeerCandidate {
            destination_hash: [0xBB; 16],
            timebase: 1000.0,
            transfer_limit: None,
            sync_limit: None,
            stamp_cost: None,
            stamp_flexibility: None,
            peering_cost: None,
            hops: Some(1),
        }));
    }

    #[test]
    fn test_resource_concluded() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let peer_hash = [0xAA; 16];
        let mut peer = LxmPeer::new(peer_hash);
        peer.currently_transferring_messages = Some(vec![[0x01; 32], [0x02; 32]]);
        peer.state = PeerState::ResourceTransferring;
        router.add_peer(peer);

        router.handle_resource_concluded(&peer_hash, true);

        let peer = router.peers.get(&peer_hash).unwrap();
        assert_eq!(peer.state, PeerState::Idle);
        assert_eq!(peer.outgoing, 2);
        assert!(peer.currently_transferring_messages.is_none());
    }

    #[test]
    fn test_control_status() {
        let config = RouterConfig {
            propagation_enabled: true,
            ..Default::default()
        };
        let mut router = LxmRouter::new(config);
        router.propagation_start_time = Some(now_f64());

        let stats = router.control_status();
        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert_eq!(stats.total_peers, 0);
        assert_eq!(stats.message_count, 0);
    }

    #[test]
    fn test_processing_limit() {
        let mut config = RouterConfig::default();
        config.ext.processing_limit = Some(1);
        let mut router = LxmRouter::new(config);

        router.send(LxMessage::new(
            [0x01; 16],
            [0; 16],
            "a",
            "b",
            DeliveryMethod::Direct,
        ));
        router.send(LxMessage::new(
            [0x02; 16],
            [0; 16],
            "c",
            "d",
            DeliveryMethod::Direct,
        ));

        let actions = router.process_outbound();
        assert_eq!(actions.len(), 1);
        assert_eq!(router.pending_outbound.len(), 1);
    }

    #[test]
    fn test_throttle_peer() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let hash = [0xAA; 16];

        assert!(!router.is_peer_throttled(&hash));
        router.throttle_peer(hash);
        assert!(router.is_peer_throttled(&hash));
    }

    #[test]
    fn test_opportunistic_fallback_to_direct() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let large_content = "x".repeat(500);
        let msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Large",
            &large_content,
            DeliveryMethod::Opportunistic,
        );
        router.send(msg);

        assert_eq!(router.pending_outbound[0].method, DeliveryMethod::Direct);
    }

    #[test]
    fn test_direct_message_state_sending() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Direct",
            "Content",
            DeliveryMethod::Direct,
        );
        router.send(msg);

        let actions = router.process_outbound();
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OutboundAction::DeliverDirect { message, .. } => {
                assert_eq!(message.state, MessageState::Sending);
            }
            _ => panic!("expected DeliverDirect"),
        }
    }

    #[test]
    fn test_propagated_message_state_sending() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let peer_hash = [0x11; 16];
        let peer = crate::peer::LxmPeer::new(peer_hash);
        router.add_peer(peer);
        router.set_outbound_propagation_node(Some(peer_hash));

        let msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Propagated",
            "Content",
            DeliveryMethod::Propagated,
        );
        router.send(msg);

        let actions = router.process_outbound();
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OutboundAction::DeliverPropagated { message, .. } => {
                assert_eq!(message.state, MessageState::Sending);
            }
            _ => panic!("expected DeliverPropagated"),
        }
    }

    #[test]
    fn test_opportunistic_message_state_sent() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Opportunistic",
            "Content",
            DeliveryMethod::Opportunistic,
        );
        router.send(msg);

        let actions = router.process_outbound();
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            OutboundAction::DeliverOpportunistic { message, .. } => {
                assert_eq!(message.state, MessageState::Sent);
                assert_eq!(message.progress, 0.50);
            }
            _ => panic!("expected DeliverOpportunistic"),
        }
    }

    #[test]
    fn test_direct_message_left_for_link_delivery_after_execute() {
        let mut router = LxmRouter::new(RouterConfig::default());
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        router.set_transport(tx);

        let signing_key = rns_crypto::ed25519::Ed25519PrivateKey::generate();
        let mut msg = LxMessage::new(
            [0xAA; 16],
            [0xBB; 16],
            "Direct Sent",
            "Content",
            DeliveryMethod::Direct,
        );
        msg.sign(&signing_key).unwrap();
        router.send(msg);

        router.tick();

        assert!(
            rx.try_recv().is_err(),
            "Direct delivery requires LinkDeliveryManager, not router.execute_actions"
        );
        assert_eq!(
            router.pending_outbound.len(),
            1,
            "core tick must leave Direct messages queued for LinkDeliveryManager"
        );
        assert_eq!(
            router.pending_outbound[0].state,
            MessageState::Outbound,
            "core tick without a Direct adapter must not claim link sending started"
        );
    }
}
