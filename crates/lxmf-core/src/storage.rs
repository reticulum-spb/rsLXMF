//! Durable LXMF storage abstraction.

use std::collections::HashMap;
use std::fmt;
#[cfg(feature = "sqlite")]
use std::path::Path;

use crate::types::PropagationTransientId;

#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    Database(String),
    InvalidData(String),
    ActorUnavailable,
    UnsupportedSchema { found: u32, supported: u32 },
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "storage I/O error: {error}"),
            Self::Database(error) => write!(formatter, "storage database error: {error}"),
            Self::InvalidData(error) => write!(formatter, "invalid storage data: {error}"),
            Self::ActorUnavailable => formatter.write_str("storage actor unavailable"),
            Self::UnsupportedSchema { found, supported } => write!(
                formatter,
                "database schema version {found} is newer than supported version {supported}"
            ),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i64)]
pub enum TransientIdKind {
    LocallyDelivered = 1,
    LocallyProcessed = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMessageMetadata {
    pub transient_id: PropagationTransientId,
    pub message_hash: [u8; 32],
    pub destination_hash: [u8; 16],
    pub stored_at: i64,
    pub stamp_value: u16,
    pub payload_size: usize,
    pub collected: bool,
    pub stamped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMessage {
    pub metadata: StoredMessageMetadata,
    pub payload: Vec<u8>,
}

impl StoredMessage {
    pub fn new(
        transient_id: PropagationTransientId,
        message_hash: [u8; 32],
        destination_hash: [u8; 16],
        stored_at: i64,
        stamp_value: u16,
        payload: Vec<u8>,
        stamped: bool,
    ) -> Self {
        Self {
            metadata: StoredMessageMetadata {
                transient_id,
                message_hash,
                destination_hash,
                stored_at,
                stamp_value,
                payload_size: payload.len(),
                collected: false,
                stamped,
            },
            payload,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MessageStoreStats {
    pub count: usize,
    pub payload_size: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredOutboundMetadata {
    pub message_id: [u8; 32],
    pub destination_hash: [u8; 16],
    pub state: u8,
    pub delivery_method: u8,
    pub deferred: bool,
    pub next_delivery_attempt: f64,
    pub last_delivery_attempt: f64,
    pub delivery_attempts: u32,
    pub created_at: f64,
    pub progress: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredOutboundMessage {
    pub metadata: StoredOutboundMetadata,
    pub encoded_message: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StoredStampCost {
    pub destination_hash: [u8; 16],
    pub cost: u8,
    pub recorded_at: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StoredIdentity {
    pub destination_hash: [u8; 16],
    pub public_key: [u8; 64],
    pub updated_at: f64,
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StoredRatchet {
    pub destination_hash: [u8; 16],
    pub ratchet_key: [u8; 32],
    pub received_at: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageMaintenance {
    pub database_bytes: u64,
    pub wal_bytes: u64,
    pub page_size: u64,
    pub page_count: u64,
    pub free_pages: u64,
    pub checkpointed_frames: u64,
    pub remaining_wal_frames: u64,
    pub vacuumed_pages: u64,
}

/// Synchronous because the router/storage actor owns each implementation.
pub trait LxmfStorage: Send {
    fn contains_transient_id(
        &self,
        kind: TransientIdKind,
        transient_id: &PropagationTransientId,
    ) -> Result<bool, StorageError>;

    fn upsert_transient_id(
        &mut self,
        kind: TransientIdKind,
        transient_id: PropagationTransientId,
        seen_at: i64,
    ) -> Result<(), StorageError>;

    fn upsert_transient_ids(
        &mut self,
        entries: &[(TransientIdKind, PropagationTransientId, i64)],
    ) -> Result<(), StorageError>;

    fn cull_transient_ids_before(&mut self, cutoff: i64) -> Result<usize, StorageError>;

    fn transient_id_count(&self, kind: TransientIdKind) -> Result<usize, StorageError>;

    fn insert_message(&mut self, message: &StoredMessage) -> Result<bool, StorageError>;

    fn message_metadata(
        &self,
        transient_id: &PropagationTransientId,
    ) -> Result<Option<StoredMessageMetadata>, StorageError>;

    fn message_payload(
        &self,
        transient_id: &PropagationTransientId,
    ) -> Result<Option<Vec<u8>>, StorageError>;

    fn message_metadata_page(
        &self,
        after: Option<&PropagationTransientId>,
        limit: usize,
    ) -> Result<Vec<StoredMessageMetadata>, StorageError>;

    fn message_metadata_for_destination(
        &self,
        destination_hash: &[u8; 16],
        after: Option<&PropagationTransientId>,
        limit: usize,
    ) -> Result<Vec<StoredMessageMetadata>, StorageError>;

    fn remove_message(
        &mut self,
        transient_id: &PropagationTransientId,
    ) -> Result<bool, StorageError>;

    fn set_message_collected(
        &mut self,
        transient_id: &PropagationTransientId,
        collected: bool,
    ) -> Result<bool, StorageError>;

    fn message_store_stats(&self) -> Result<MessageStoreStats, StorageError>;

    fn remove_messages_stored_before(
        &mut self,
        cutoff: i64,
        limit: usize,
    ) -> Result<usize, StorageError>;

    /// Remove the highest weighted messages in one atomic batch, stopping
    /// after at least `bytes_to_remove` bytes or `limit` rows are selected.
    fn remove_messages_by_weight(
        &mut self,
        bytes_to_remove: usize,
        now: i64,
        prioritised_destinations: &[[u8; 16]],
        limit: usize,
    ) -> Result<usize, StorageError>;

    fn upsert_outbound_message(
        &mut self,
        message: &StoredOutboundMessage,
    ) -> Result<(), StorageError>;

    fn outbound_message(
        &self,
        message_id: &[u8; 32],
    ) -> Result<Option<StoredOutboundMessage>, StorageError>;

    fn outbound_ready(
        &self,
        now: f64,
        deferred: bool,
        limit: usize,
    ) -> Result<Vec<StoredOutboundMetadata>, StorageError>;

    fn outbound_metadata_page(
        &self,
        deferred: bool,
        limit: usize,
    ) -> Result<Vec<StoredOutboundMetadata>, StorageError>;

    fn update_outbound_delivery(
        &mut self,
        metadata: &StoredOutboundMetadata,
    ) -> Result<bool, StorageError>;

    fn remove_outbound_message(&mut self, message_id: &[u8; 32]) -> Result<bool, StorageError>;

    fn outbound_count(&self, deferred: Option<bool>) -> Result<usize, StorageError>;

    fn upsert_ticket(&mut self, ticket: &crate::ticket::Ticket) -> Result<(), StorageError>;
    fn valid_ticket(
        &self,
        destination_hash: &[u8; 16],
        now: f64,
    ) -> Result<Option<crate::ticket::Ticket>, StorageError>;
    fn remove_tickets(&mut self, destination_hash: &[u8; 16]) -> Result<usize, StorageError>;
    fn cull_tickets(&mut self, cutoff: f64) -> Result<usize, StorageError>;
    fn upsert_stamp_cost(&mut self, entry: StoredStampCost) -> Result<(), StorageError>;
    fn stamp_cost(
        &self,
        destination_hash: &[u8; 16],
    ) -> Result<Option<StoredStampCost>, StorageError>;
    fn remove_stamp_cost(&mut self, destination_hash: &[u8; 16]) -> Result<bool, StorageError>;
    fn cull_stamp_costs_before(&mut self, cutoff: f64) -> Result<usize, StorageError>;
    fn stamp_cost_count(&self) -> Result<usize, StorageError>;
    fn upsert_identity(&mut self, identity: StoredIdentity) -> Result<(), StorageError>;
    fn identity(&self, destination_hash: &[u8; 16])
    -> Result<Option<StoredIdentity>, StorageError>;
    fn identity_page(&self, limit: usize) -> Result<Vec<StoredIdentity>, StorageError>;
    fn upsert_ratchet(&mut self, ratchet: StoredRatchet) -> Result<(), StorageError>;
    fn ratchet(&self, destination_hash: &[u8; 16]) -> Result<Option<StoredRatchet>, StorageError>;
    fn ratchet_page(&self, cutoff: f64, limit: usize) -> Result<Vec<StoredRatchet>, StorageError>;
    fn cull_ratchets_before(&mut self, cutoff: f64) -> Result<usize, StorageError>;
    fn put_state_blob(&mut self, key: &str, value: &[u8]) -> Result<(), StorageError>;
    fn put_state_blobs(&mut self, entries: &[(&str, &[u8])]) -> Result<(), StorageError> {
        for (key, value) in entries {
            self.put_state_blob(key, value)?;
        }
        Ok(())
    }
    fn state_blob(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError>;
    fn insert_inbound_message(
        &mut self,
        message_id: [u8; 32],
        received_at: f64,
        encoded: &[u8],
    ) -> Result<(), StorageError>;
    fn contains_inbound_message(&self, message_id: &[u8; 32]) -> Result<bool, StorageError>;
    fn replace_peers(&mut self, peers: &[([u8; 16], Vec<u8>)]) -> Result<(), StorageError>;
    fn peer_page(&self, limit: usize) -> Result<Vec<([u8; 16], Vec<u8>)>, StorageError>;
    fn maintain(&mut self, vacuum_pages: u32) -> Result<StorageMaintenance, StorageError>;
}

#[derive(Debug, Default)]
pub struct MemoryStorage {
    transient_ids: HashMap<(TransientIdKind, PropagationTransientId), i64>,
    messages: HashMap<PropagationTransientId, StoredMessage>,
    outbound_messages: HashMap<[u8; 32], StoredOutboundMessage>,
    tickets: HashMap<[u8; 16], crate::ticket::Ticket>,
    stamp_costs: HashMap<[u8; 16], StoredStampCost>,
    identities: HashMap<[u8; 16], StoredIdentity>,
    ratchets: HashMap<[u8; 16], StoredRatchet>,
    state_blobs: HashMap<String, Vec<u8>>,
    inbound_messages: HashMap<[u8; 32], (f64, Vec<u8>)>,
    peers: HashMap<[u8; 16], Vec<u8>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LxmfStorage for MemoryStorage {
    fn contains_transient_id(
        &self,
        kind: TransientIdKind,
        transient_id: &PropagationTransientId,
    ) -> Result<bool, StorageError> {
        Ok(self.transient_ids.contains_key(&(kind, *transient_id)))
    }
    fn upsert_transient_id(
        &mut self,
        kind: TransientIdKind,
        transient_id: PropagationTransientId,
        seen_at: i64,
    ) -> Result<(), StorageError> {
        self.transient_ids.insert((kind, transient_id), seen_at);
        Ok(())
    }

    fn upsert_transient_ids(
        &mut self,
        entries: &[(TransientIdKind, PropagationTransientId, i64)],
    ) -> Result<(), StorageError> {
        for (kind, transient_id, seen_at) in entries {
            self.transient_ids.insert((*kind, *transient_id), *seen_at);
        }
        Ok(())
    }

    fn cull_transient_ids_before(&mut self, cutoff: i64) -> Result<usize, StorageError> {
        let before = self.transient_ids.len();
        self.transient_ids.retain(|_, seen_at| *seen_at >= cutoff);
        Ok(before - self.transient_ids.len())
    }

    fn transient_id_count(&self, kind: TransientIdKind) -> Result<usize, StorageError> {
        Ok(self
            .transient_ids
            .keys()
            .filter(|(entry_kind, _)| *entry_kind == kind)
            .count())
    }

    fn insert_message(&mut self, message: &StoredMessage) -> Result<bool, StorageError> {
        validate_message(message)?;
        if self.messages.contains_key(&message.metadata.transient_id) {
            return Ok(false);
        }
        self.messages
            .insert(message.metadata.transient_id, message.clone());
        Ok(true)
    }

    fn message_metadata(
        &self,
        transient_id: &PropagationTransientId,
    ) -> Result<Option<StoredMessageMetadata>, StorageError> {
        Ok(self
            .messages
            .get(transient_id)
            .map(|message| message.metadata.clone()))
    }

    fn message_payload(
        &self,
        transient_id: &PropagationTransientId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self
            .messages
            .get(transient_id)
            .map(|message| message.payload.clone()))
    }

    fn message_metadata_page(
        &self,
        after: Option<&PropagationTransientId>,
        limit: usize,
    ) -> Result<Vec<StoredMessageMetadata>, StorageError> {
        Ok(metadata_page(
            self.messages.values().map(|message| &message.metadata),
            after,
            limit,
        ))
    }

    fn message_metadata_for_destination(
        &self,
        destination_hash: &[u8; 16],
        after: Option<&PropagationTransientId>,
        limit: usize,
    ) -> Result<Vec<StoredMessageMetadata>, StorageError> {
        Ok(metadata_page(
            self.messages
                .values()
                .map(|message| &message.metadata)
                .filter(|metadata| &metadata.destination_hash == destination_hash),
            after,
            limit,
        ))
    }

    fn remove_message(
        &mut self,
        transient_id: &PropagationTransientId,
    ) -> Result<bool, StorageError> {
        Ok(self.messages.remove(transient_id).is_some())
    }

    fn set_message_collected(
        &mut self,
        transient_id: &PropagationTransientId,
        collected: bool,
    ) -> Result<bool, StorageError> {
        let Some(message) = self.messages.get_mut(transient_id) else {
            return Ok(false);
        };
        message.metadata.collected = collected;
        Ok(true)
    }

    fn message_store_stats(&self) -> Result<MessageStoreStats, StorageError> {
        Ok(MessageStoreStats {
            count: self.messages.len(),
            payload_size: self
                .messages
                .values()
                .map(|message| message.metadata.payload_size)
                .sum(),
        })
    }

    fn remove_messages_stored_before(
        &mut self,
        cutoff: i64,
        limit: usize,
    ) -> Result<usize, StorageError> {
        let mut candidates = self
            .messages
            .values()
            .filter(|message| message.metadata.stored_at < cutoff)
            .map(|message| (message.metadata.stored_at, message.metadata.transient_id))
            .collect::<Vec<_>>();
        candidates.sort_unstable();
        candidates.truncate(limit);
        let count = candidates.len();
        for (_, transient_id) in candidates {
            self.messages.remove(&transient_id);
        }
        Ok(count)
    }

    fn remove_messages_by_weight(
        &mut self,
        bytes_to_remove: usize,
        now: i64,
        prioritised_destinations: &[[u8; 16]],
        limit: usize,
    ) -> Result<usize, StorageError> {
        let prioritised = prioritised_destinations
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        let mut candidates = self
            .messages
            .values()
            .map(|message| {
                let metadata = &message.metadata;
                let age_weight = (((now - metadata.stored_at) as f64) / 345_600.0).max(1.0);
                let priority_weight = if prioritised.contains(&metadata.destination_hash) {
                    0.1
                } else {
                    1.0
                };
                (
                    metadata.transient_id,
                    metadata.payload_size,
                    priority_weight * age_weight * metadata.payload_size as f64,
                )
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            right
                .2
                .total_cmp(&left.2)
                .then_with(|| left.0.cmp(&right.0))
        });

        let mut selected = Vec::new();
        let mut selected_bytes = 0usize;
        for (transient_id, payload_size, _) in candidates.into_iter().take(limit) {
            if selected_bytes >= bytes_to_remove {
                break;
            }
            selected.push(transient_id);
            selected_bytes = selected_bytes.saturating_add(payload_size);
        }
        for transient_id in &selected {
            self.messages.remove(transient_id);
        }
        Ok(selected.len())
    }

    fn upsert_outbound_message(
        &mut self,
        message: &StoredOutboundMessage,
    ) -> Result<(), StorageError> {
        self.outbound_messages
            .insert(message.metadata.message_id, message.clone());
        Ok(())
    }

    fn outbound_message(
        &self,
        message_id: &[u8; 32],
    ) -> Result<Option<StoredOutboundMessage>, StorageError> {
        Ok(self.outbound_messages.get(message_id).cloned())
    }

    fn outbound_ready(
        &self,
        now: f64,
        deferred: bool,
        limit: usize,
    ) -> Result<Vec<StoredOutboundMetadata>, StorageError> {
        let mut ready = self
            .outbound_messages
            .values()
            .filter(|message| {
                message.metadata.deferred == deferred
                    && message.metadata.next_delivery_attempt <= now
            })
            .map(|message| message.metadata.clone())
            .collect::<Vec<_>>();
        ready.sort_by(|left, right| {
            left.metadata_order_key()
                .partial_cmp(&right.metadata_order_key())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.message_id.cmp(&right.message_id))
        });
        ready.truncate(limit);
        Ok(ready)
    }

    fn outbound_metadata_page(
        &self,
        deferred: bool,
        limit: usize,
    ) -> Result<Vec<StoredOutboundMetadata>, StorageError> {
        let mut entries = self
            .outbound_messages
            .values()
            .filter(|message| message.metadata.deferred == deferred)
            .map(|message| message.metadata.clone())
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            left.created_at
                .total_cmp(&right.created_at)
                .then_with(|| left.message_id.cmp(&right.message_id))
        });
        entries.truncate(limit);
        Ok(entries)
    }

    fn update_outbound_delivery(
        &mut self,
        metadata: &StoredOutboundMetadata,
    ) -> Result<bool, StorageError> {
        let Some(message) = self.outbound_messages.get_mut(&metadata.message_id) else {
            return Ok(false);
        };
        message.metadata = metadata.clone();
        Ok(true)
    }

    fn remove_outbound_message(&mut self, message_id: &[u8; 32]) -> Result<bool, StorageError> {
        Ok(self.outbound_messages.remove(message_id).is_some())
    }

    fn outbound_count(&self, deferred: Option<bool>) -> Result<usize, StorageError> {
        Ok(self
            .outbound_messages
            .values()
            .filter(|message| deferred.is_none_or(|value| message.metadata.deferred == value))
            .count())
    }

    fn upsert_ticket(&mut self, ticket: &crate::ticket::Ticket) -> Result<(), StorageError> {
        self.tickets.insert(ticket.token, ticket.clone());
        Ok(())
    }

    fn valid_ticket(
        &self,
        destination_hash: &[u8; 16],
        now: f64,
    ) -> Result<Option<crate::ticket::Ticket>, StorageError> {
        Ok(self
            .tickets
            .values()
            .find(|ticket| ticket.destination_hash == *destination_hash && ticket.is_valid(now))
            .cloned())
    }

    fn remove_tickets(&mut self, destination_hash: &[u8; 16]) -> Result<usize, StorageError> {
        let before = self.tickets.len();
        self.tickets
            .retain(|_, ticket| ticket.destination_hash != *destination_hash);
        Ok(before - self.tickets.len())
    }

    fn cull_tickets(&mut self, cutoff: f64) -> Result<usize, StorageError> {
        let before = self.tickets.len();
        self.tickets
            .retain(|_, ticket| !ticket.used && ticket.expires >= cutoff);
        Ok(before - self.tickets.len())
    }

    fn upsert_stamp_cost(&mut self, entry: StoredStampCost) -> Result<(), StorageError> {
        self.stamp_costs.insert(entry.destination_hash, entry);
        Ok(())
    }

    fn stamp_cost(
        &self,
        destination_hash: &[u8; 16],
    ) -> Result<Option<StoredStampCost>, StorageError> {
        Ok(self.stamp_costs.get(destination_hash).copied())
    }

    fn remove_stamp_cost(&mut self, destination_hash: &[u8; 16]) -> Result<bool, StorageError> {
        Ok(self.stamp_costs.remove(destination_hash).is_some())
    }

    fn cull_stamp_costs_before(&mut self, cutoff: f64) -> Result<usize, StorageError> {
        let before = self.stamp_costs.len();
        self.stamp_costs
            .retain(|_, entry| entry.recorded_at >= cutoff);
        Ok(before - self.stamp_costs.len())
    }
    fn stamp_cost_count(&self) -> Result<usize, StorageError> {
        Ok(self.stamp_costs.len())
    }
    fn upsert_identity(&mut self, identity: StoredIdentity) -> Result<(), StorageError> {
        self.identities.insert(identity.destination_hash, identity);
        Ok(())
    }
    fn identity(
        &self,
        destination_hash: &[u8; 16],
    ) -> Result<Option<StoredIdentity>, StorageError> {
        Ok(self.identities.get(destination_hash).copied())
    }
    fn identity_page(&self, limit: usize) -> Result<Vec<StoredIdentity>, StorageError> {
        let mut values = self.identities.values().copied().collect::<Vec<_>>();
        values.sort_by(|a, b| b.updated_at.total_cmp(&a.updated_at));
        values.truncate(limit);
        Ok(values)
    }
    fn upsert_ratchet(&mut self, ratchet: StoredRatchet) -> Result<(), StorageError> {
        self.ratchets.insert(ratchet.destination_hash, ratchet);
        Ok(())
    }
    fn ratchet(&self, destination_hash: &[u8; 16]) -> Result<Option<StoredRatchet>, StorageError> {
        Ok(self.ratchets.get(destination_hash).copied())
    }
    fn ratchet_page(&self, cutoff: f64, limit: usize) -> Result<Vec<StoredRatchet>, StorageError> {
        let mut values = self
            .ratchets
            .values()
            .filter(|r| r.received_at >= cutoff)
            .copied()
            .collect::<Vec<_>>();
        values.sort_by(|a, b| b.received_at.total_cmp(&a.received_at));
        values.truncate(limit);
        Ok(values)
    }
    fn cull_ratchets_before(&mut self, cutoff: f64) -> Result<usize, StorageError> {
        let before = self.ratchets.len();
        self.ratchets.retain(|_, r| r.received_at >= cutoff);
        Ok(before - self.ratchets.len())
    }
    fn put_state_blob(&mut self, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.state_blobs.insert(key.to_string(), value.to_vec());
        Ok(())
    }
    fn state_blob(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.state_blobs.get(key).cloned())
    }
    fn insert_inbound_message(
        &mut self,
        id: [u8; 32],
        at: f64,
        encoded: &[u8],
    ) -> Result<(), StorageError> {
        self.inbound_messages.insert(id, (at, encoded.to_vec()));
        Ok(())
    }
    fn contains_inbound_message(&self, message_id: &[u8; 32]) -> Result<bool, StorageError> {
        Ok(self.inbound_messages.contains_key(message_id))
    }
    fn replace_peers(&mut self, peers: &[([u8; 16], Vec<u8>)]) -> Result<(), StorageError> {
        self.peers = peers.iter().cloned().collect();
        Ok(())
    }
    fn peer_page(&self, limit: usize) -> Result<Vec<([u8; 16], Vec<u8>)>, StorageError> {
        Ok(self
            .peers
            .iter()
            .take(limit)
            .map(|(h, v)| (*h, v.clone()))
            .collect())
    }
    fn maintain(&mut self, _vacuum_pages: u32) -> Result<StorageMaintenance, StorageError> {
        Ok(StorageMaintenance::default())
    }
}

impl StoredOutboundMetadata {
    fn metadata_order_key(&self) -> f64 {
        self.next_delivery_attempt
    }
}

fn validate_message(message: &StoredMessage) -> Result<(), StorageError> {
    if message.metadata.payload_size != message.payload.len() {
        return Err(StorageError::InvalidData(format!(
            "payload_size {} does not match payload length {}",
            message.metadata.payload_size,
            message.payload.len()
        )));
    }
    Ok(())
}

fn metadata_page<'a>(
    entries: impl Iterator<Item = &'a StoredMessageMetadata>,
    after: Option<&PropagationTransientId>,
    limit: usize,
) -> Vec<StoredMessageMetadata> {
    let mut page = entries
        .filter(|metadata| after.is_none_or(|cursor| metadata.transient_id > *cursor))
        .cloned()
        .collect::<Vec<_>>();
    page.sort_unstable_by_key(|metadata| metadata.transient_id);
    page.truncate(limit);
    page
}

mod actor;
#[cfg(feature = "sqlite")]
mod sqlite;

pub use actor::{StorageHandle, spawn_storage_actor};

#[cfg(feature = "sqlite")]
pub use actor::spawn_sqlite_storage_actor;

#[cfg(feature = "sqlite")]
pub use sqlite::SqliteStorage;

#[cfg(feature = "sqlite")]
pub fn open_sqlite(path: &Path) -> Result<SqliteStorage, StorageError> {
    SqliteStorage::open(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transient_contract(storage: &mut dyn LxmfStorage) {
        let delivered = [0x11; 32];
        let processed = [0x22; 32];
        assert!(
            !storage
                .contains_transient_id(TransientIdKind::LocallyDelivered, &delivered)
                .unwrap()
        );
        storage
            .upsert_transient_id(TransientIdKind::LocallyDelivered, delivered, 100)
            .unwrap();
        storage
            .upsert_transient_ids(&[
                (TransientIdKind::LocallyDelivered, delivered, 200),
                (TransientIdKind::LocallyProcessed, processed, 150),
                (TransientIdKind::LocallyProcessed, [0x33; 32], 174),
                (TransientIdKind::LocallyProcessed, [0x34; 32], 175),
            ])
            .unwrap();
        assert!(
            storage
                .contains_transient_id(TransientIdKind::LocallyDelivered, &delivered)
                .unwrap()
        );
        assert_eq!(
            storage
                .transient_id_count(TransientIdKind::LocallyDelivered)
                .unwrap(),
            1
        );
        assert_eq!(storage.cull_transient_ids_before(175).unwrap(), 2);
        assert!(
            !storage
                .contains_transient_id(TransientIdKind::LocallyProcessed, &processed)
                .unwrap()
        );
        assert!(
            storage
                .contains_transient_id(TransientIdKind::LocallyProcessed, &[0x34; 32])
                .unwrap()
        );
    }

    fn message_contract(storage: &mut dyn LxmfStorage) {
        let first = StoredMessage::new([1; 32], [11; 32], [21; 16], 100, 8, vec![1, 2], false);
        let second = StoredMessage::new([2; 32], [12; 32], [21; 16], 200, 9, vec![3; 3], true);
        let third = StoredMessage::new([3; 32], [13; 32], [22; 16], 300, 10, vec![4; 4], false);
        assert!(storage.insert_message(&second).unwrap());
        assert!(storage.insert_message(&first).unwrap());
        assert!(storage.insert_message(&third).unwrap());
        assert!(!storage.insert_message(&first).unwrap());

        let metadata = storage.message_metadata(&[2; 32]).unwrap().unwrap();
        assert_eq!(metadata.payload_size, 3);
        assert!(metadata.stamped);
        assert_eq!(storage.message_payload(&[2; 32]).unwrap(), Some(vec![3; 3]));

        let page = storage.message_metadata_page(None, 2).unwrap();
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].transient_id, [1; 32]);
        assert_eq!(page[1].transient_id, [2; 32]);
        let next = storage
            .message_metadata_page(Some(&page[1].transient_id), 2)
            .unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].transient_id, [3; 32]);

        let destination = storage
            .message_metadata_for_destination(&[21; 16], None, 10)
            .unwrap();
        assert_eq!(destination.len(), 2);
        assert!(storage.set_message_collected(&[1; 32], true).unwrap());
        assert!(
            storage
                .message_metadata(&[1; 32])
                .unwrap()
                .unwrap()
                .collected
        );
        assert!(!storage.set_message_collected(&[9; 32], true).unwrap());

        assert_eq!(
            storage.message_store_stats().unwrap(),
            MessageStoreStats {
                count: 3,
                payload_size: 9
            }
        );
        assert!(storage.remove_message(&[2; 32]).unwrap());
        assert!(!storage.remove_message(&[2; 32]).unwrap());
        assert_eq!(storage.message_store_stats().unwrap().count, 2);
        assert_eq!(storage.remove_messages_stored_before(300, 1).unwrap(), 1);
        assert_eq!(storage.message_store_stats().unwrap().count, 1);
        assert_eq!(storage.remove_messages_stored_before(300, 10).unwrap(), 0);
    }

    fn weighted_culling_contract(storage: &mut dyn LxmfStorage) {
        let prioritised_destination = [0xAA; 16];
        let prioritised = StoredMessage::new(
            [0x11; 32],
            [0x21; 32],
            prioritised_destination,
            1_000,
            0,
            vec![0; 50],
            false,
        );
        let ordinary = StoredMessage::new(
            [0x12; 32],
            [0x22; 32],
            [0xBB; 16],
            1_000,
            0,
            vec![0; 10],
            false,
        );
        storage.insert_message(&prioritised).unwrap();
        storage.insert_message(&ordinary).unwrap();

        assert_eq!(
            storage
                .remove_messages_by_weight(1, 1_000, &[prioritised_destination], 1)
                .unwrap(),
            1
        );
        assert!(storage.message_metadata(&[0x11; 32]).unwrap().is_some());
        assert!(storage.message_metadata(&[0x12; 32]).unwrap().is_none());
        assert_eq!(storage.message_store_stats().unwrap().payload_size, 50);
    }

    fn outbound_contract(storage: &mut dyn LxmfStorage) {
        let first = StoredOutboundMessage {
            metadata: StoredOutboundMetadata {
                message_id: [0x41; 32],
                destination_hash: [0x51; 16],
                state: 1,
                delivery_method: 2,
                deferred: false,
                next_delivery_attempt: 100.5,
                last_delivery_attempt: 90.25,
                delivery_attempts: 2,
                created_at: 50.0,
                progress: 0.25,
            },
            encoded_message: vec![0x61; 64],
        };
        let mut deferred = first.clone();
        deferred.metadata.message_id = [0x42; 32];
        deferred.metadata.deferred = true;
        deferred.metadata.next_delivery_attempt = 0.0;
        deferred.encoded_message = vec![0x62; 32];
        storage.upsert_outbound_message(&first).unwrap();
        storage.upsert_outbound_message(&deferred).unwrap();

        assert_eq!(storage.outbound_count(None).unwrap(), 2);
        assert_eq!(storage.outbound_count(Some(false)).unwrap(), 1);
        assert!(storage.outbound_ready(100.0, false, 8).unwrap().is_empty());
        assert_eq!(
            storage.outbound_ready(101.0, false, 8).unwrap(),
            vec![first.metadata.clone()]
        );
        assert_eq!(
            storage.outbound_ready(1.0, true, 8).unwrap(),
            vec![deferred.metadata.clone()]
        );
        assert_eq!(
            storage.outbound_message(&[0x41; 32]).unwrap(),
            Some(first.clone())
        );

        let mut updated = first.metadata.clone();
        updated.delivery_attempts = 3;
        updated.progress = 0.75;
        updated.next_delivery_attempt = 200.0;
        assert!(storage.update_outbound_delivery(&updated).unwrap());
        assert_eq!(
            storage
                .outbound_message(&[0x41; 32])
                .unwrap()
                .unwrap()
                .metadata,
            updated
        );
        assert!(storage.remove_outbound_message(&[0x41; 32]).unwrap());
        assert!(!storage.remove_outbound_message(&[0x41; 32]).unwrap());
    }

    fn ticket_and_stamp_cost_contract(storage: &mut dyn LxmfStorage) {
        let destination = [0x71; 16];
        let ticket = crate::ticket::Ticket::new([0x72; 16], destination, 200.0);
        storage.upsert_ticket(&ticket).unwrap();
        assert_eq!(
            storage
                .valid_ticket(&destination, 100.0)
                .unwrap()
                .unwrap()
                .token,
            ticket.token
        );
        assert!(storage.valid_ticket(&destination, 201.0).unwrap().is_none());
        assert_eq!(storage.remove_tickets(&destination).unwrap(), 1);

        let entry = StoredStampCost {
            destination_hash: destination,
            cost: 9,
            recorded_at: 100.0,
        };
        storage.upsert_stamp_cost(entry).unwrap();
        assert_eq!(storage.stamp_cost(&destination).unwrap(), Some(entry));
        assert_eq!(storage.cull_stamp_costs_before(101.0).unwrap(), 1);
        assert!(storage.stamp_cost(&destination).unwrap().is_none());
    }

    fn identity_and_ratchet_contract(storage: &mut dyn LxmfStorage) {
        let identity = StoredIdentity {
            destination_hash: [0x81; 16],
            public_key: [0x82; 64],
            updated_at: 10.0,
        };
        storage.upsert_identity(identity).unwrap();
        assert_eq!(
            storage.identity(&identity.destination_hash).unwrap(),
            Some(identity)
        );
        assert_eq!(storage.identity_page(1).unwrap(), vec![identity]);
        let first = StoredRatchet {
            destination_hash: identity.destination_hash,
            ratchet_key: [0x83; 32],
            received_at: 20.0,
        };
        storage.upsert_ratchet(first).unwrap();
        let replacement = StoredRatchet {
            ratchet_key: [0x84; 32],
            received_at: 30.0,
            ..first
        };
        storage.upsert_ratchet(replacement).unwrap();
        assert_eq!(
            storage.ratchet(&identity.destination_hash).unwrap(),
            Some(replacement)
        );
        assert_eq!(storage.ratchet_page(25.0, 1).unwrap(), vec![replacement]);
        assert_eq!(storage.cull_ratchets_before(31.0).unwrap(), 1);
    }

    #[test]
    fn memory_storage_contract() {
        let mut storage = MemoryStorage::new();
        transient_contract(&mut storage);
        message_contract(&mut storage);
        let mut weighted_storage = MemoryStorage::new();
        weighted_culling_contract(&mut weighted_storage);
        let mut outbound_storage = MemoryStorage::new();
        outbound_contract(&mut outbound_storage);
        let mut durable_state = MemoryStorage::new();
        ticket_and_stamp_cost_contract(&mut durable_state);
        identity_and_ratchet_contract(&mut durable_state);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_storage_contract() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lxmf.sqlite");
        let mut storage = SqliteStorage::open(&path).unwrap();
        transient_contract(&mut storage);
        message_contract(&mut storage);
        let weighted_path = directory.path().join("weighted.sqlite");
        let mut weighted_storage = SqliteStorage::open(&weighted_path).unwrap();
        weighted_culling_contract(&mut weighted_storage);
        let outbound_path = directory.path().join("outbound.sqlite");
        let mut outbound_storage = SqliteStorage::open(&outbound_path).unwrap();
        outbound_contract(&mut outbound_storage);
        let durable_path = directory.path().join("durable-state.sqlite");
        let mut durable_state = SqliteStorage::open(&durable_path).unwrap();
        ticket_and_stamp_cost_contract(&mut durable_state);
        identity_and_ratchet_contract(&mut durable_state);

        storage
            .upsert_transient_id(TransientIdKind::LocallyDelivered, [0x44; 32], 300)
            .unwrap();
        drop(storage);
        let reopened = SqliteStorage::open(&path).unwrap();
        assert!(
            reopened
                .contains_transient_id(TransientIdKind::LocallyDelivered, &[0x44; 32])
                .unwrap()
        );
        assert_eq!(reopened.schema_version().unwrap(), 7);
        assert_eq!(reopened.message_store_stats().unwrap().count, 1);
    }
}
