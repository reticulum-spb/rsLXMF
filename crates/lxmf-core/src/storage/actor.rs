#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use super::{
    LxmfStorage, MessageStoreStats, StorageError, StoredIdentity, StoredMessage,
    StoredMessageMetadata, StoredOutboundMessage, StoredOutboundMetadata, StoredRatchet,
    StoredStampCost, TransientIdKind,
};
use crate::types::PropagationTransientId;

type StorageOperation = Box<dyn FnOnce(&mut dyn LxmfStorage) + Send>;

/// Cloneable synchronous facade for a single dedicated storage worker.
#[derive(Clone)]
pub struct StorageHandle {
    inner: Arc<ActorInner>,
}

impl std::fmt::Debug for StorageHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageHandle")
            .finish_non_exhaustive()
    }
}

struct ActorInner {
    sender: Mutex<Option<mpsc::Sender<StorageOperation>>>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    #[cfg(test)]
    injected_failures: AtomicUsize,
}

impl ActorInner {
    fn new(sender: mpsc::Sender<StorageOperation>, worker: std::thread::JoinHandle<()>) -> Self {
        Self {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(Some(worker)),
            #[cfg(test)]
            injected_failures: AtomicUsize::new(0),
        }
    }
}

impl Drop for ActorInner {
    fn drop(&mut self) {
        self.sender.get_mut().ok().and_then(Option::take);
        if let Some(worker) = self.worker.get_mut().ok().and_then(Option::take) {
            let _ = worker.join();
        }
    }
}

impl StorageHandle {
    fn call<T, F>(&self, operation: F) -> Result<T, StorageError>
    where
        T: Send + 'static,
        F: FnOnce(&mut dyn LxmfStorage) -> Result<T, StorageError> + Send + 'static,
    {
        #[cfg(test)]
        if self
            .inner
            .injected_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(StorageError::ActorUnavailable);
        }
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        let sender = self
            .inner
            .sender
            .lock()
            .map_err(|_| StorageError::ActorUnavailable)?
            .as_ref()
            .cloned()
            .ok_or(StorageError::ActorUnavailable)?;
        sender
            .send(Box::new(move |storage| {
                let _ = reply_tx.send(operation(storage));
            }))
            .map_err(|_| StorageError::ActorUnavailable)?;
        reply_rx
            .recv()
            .map_err(|_| StorageError::ActorUnavailable)?
    }

    /// Inject deterministic operation failures in tests without changing the
    /// wrapped backend or killing its worker.
    #[cfg(test)]
    pub(crate) fn fail_next_operations(&self, count: usize) {
        self.inner.injected_failures.store(count, Ordering::SeqCst);
    }
}

pub fn spawn_storage_actor<S>(storage: S) -> Result<StorageHandle, StorageError>
where
    S: LxmfStorage + 'static,
{
    let (sender, receiver) = mpsc::channel::<StorageOperation>();
    let worker = std::thread::Builder::new()
        .name("lxmf-storage".into())
        .spawn(move || run_worker(Box::new(storage), receiver))
        .map_err(StorageError::Io)?;
    Ok(StorageHandle {
        inner: Arc::new(ActorInner::new(sender, worker)),
    })
}

#[cfg(feature = "sqlite")]
pub fn spawn_sqlite_storage_actor(path: std::path::PathBuf) -> Result<StorageHandle, StorageError> {
    let (sender, receiver) = mpsc::channel::<StorageOperation>();
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let worker = std::thread::Builder::new()
        .name("lxmf-sqlite-storage".into())
        .spawn(move || match super::SqliteStorage::open(&path) {
            Ok(storage) => {
                let _ = ready_tx.send(Ok(()));
                run_worker(Box::new(storage), receiver);
            }
            Err(error) => {
                let _ = ready_tx.send(Err(error));
            }
        })
        .map_err(StorageError::Io)?;
    ready_rx
        .recv()
        .map_err(|_| StorageError::ActorUnavailable)??;
    Ok(StorageHandle {
        inner: Arc::new(ActorInner::new(sender, worker)),
    })
}

fn run_worker(mut storage: Box<dyn LxmfStorage>, receiver: mpsc::Receiver<StorageOperation>) {
    while let Ok(operation) = receiver.recv() {
        operation(storage.as_mut());
    }
}

impl LxmfStorage for StorageHandle {
    fn contains_transient_id(
        &self,
        kind: TransientIdKind,
        transient_id: &PropagationTransientId,
    ) -> Result<bool, StorageError> {
        let transient_id = *transient_id;
        self.call(move |storage| storage.contains_transient_id(kind, &transient_id))
    }

    fn upsert_transient_id(
        &mut self,
        kind: TransientIdKind,
        transient_id: PropagationTransientId,
        seen_at: i64,
    ) -> Result<(), StorageError> {
        self.call(move |storage| storage.upsert_transient_id(kind, transient_id, seen_at))
    }

    fn upsert_transient_ids(
        &mut self,
        entries: &[(TransientIdKind, PropagationTransientId, i64)],
    ) -> Result<(), StorageError> {
        let entries = entries.to_vec();
        self.call(move |storage| storage.upsert_transient_ids(&entries))
    }

    fn cull_transient_ids_before(&mut self, cutoff: i64) -> Result<usize, StorageError> {
        self.call(move |storage| storage.cull_transient_ids_before(cutoff))
    }

    fn transient_id_count(&self, kind: TransientIdKind) -> Result<usize, StorageError> {
        self.call(move |storage| storage.transient_id_count(kind))
    }

    fn insert_message(&mut self, message: &StoredMessage) -> Result<bool, StorageError> {
        let message = message.clone();
        self.call(move |storage| storage.insert_message(&message))
    }

    fn message_metadata(
        &self,
        transient_id: &PropagationTransientId,
    ) -> Result<Option<StoredMessageMetadata>, StorageError> {
        let transient_id = *transient_id;
        self.call(move |storage| storage.message_metadata(&transient_id))
    }

    fn message_payload(
        &self,
        transient_id: &PropagationTransientId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let transient_id = *transient_id;
        self.call(move |storage| storage.message_payload(&transient_id))
    }

    fn message_metadata_page(
        &self,
        after: Option<&PropagationTransientId>,
        limit: usize,
    ) -> Result<Vec<StoredMessageMetadata>, StorageError> {
        let after = after.copied();
        self.call(move |storage| storage.message_metadata_page(after.as_ref(), limit))
    }

    fn message_metadata_for_destination(
        &self,
        destination_hash: &[u8; 16],
        after: Option<&PropagationTransientId>,
        limit: usize,
    ) -> Result<Vec<StoredMessageMetadata>, StorageError> {
        let destination_hash = *destination_hash;
        let after = after.copied();
        self.call(move |storage| {
            storage.message_metadata_for_destination(&destination_hash, after.as_ref(), limit)
        })
    }

    fn remove_message(
        &mut self,
        transient_id: &PropagationTransientId,
    ) -> Result<bool, StorageError> {
        let transient_id = *transient_id;
        self.call(move |storage| storage.remove_message(&transient_id))
    }

    fn set_message_collected(
        &mut self,
        transient_id: &PropagationTransientId,
        collected: bool,
    ) -> Result<bool, StorageError> {
        let transient_id = *transient_id;
        self.call(move |storage| storage.set_message_collected(&transient_id, collected))
    }

    fn message_store_stats(&self) -> Result<MessageStoreStats, StorageError> {
        self.call(|storage| storage.message_store_stats())
    }

    fn remove_messages_stored_before(
        &mut self,
        cutoff: i64,
        limit: usize,
    ) -> Result<usize, StorageError> {
        self.call(move |storage| storage.remove_messages_stored_before(cutoff, limit))
    }

    fn remove_messages_by_weight(
        &mut self,
        bytes_to_remove: usize,
        now: i64,
        prioritised_destinations: &[[u8; 16]],
        limit: usize,
    ) -> Result<usize, StorageError> {
        let prioritised_destinations = prioritised_destinations.to_vec();
        self.call(move |storage| {
            storage.remove_messages_by_weight(
                bytes_to_remove,
                now,
                &prioritised_destinations,
                limit,
            )
        })
    }

    fn upsert_outbound_message(
        &mut self,
        message: &StoredOutboundMessage,
    ) -> Result<(), StorageError> {
        let message = message.clone();
        self.call(move |storage| storage.upsert_outbound_message(&message))
    }

    fn outbound_message(
        &self,
        message_id: &[u8; 32],
    ) -> Result<Option<StoredOutboundMessage>, StorageError> {
        let message_id = *message_id;
        self.call(move |storage| storage.outbound_message(&message_id))
    }

    fn outbound_ready(
        &self,
        now: f64,
        deferred: bool,
        limit: usize,
    ) -> Result<Vec<StoredOutboundMetadata>, StorageError> {
        self.call(move |storage| storage.outbound_ready(now, deferred, limit))
    }

    fn outbound_metadata_page(
        &self,
        deferred: bool,
        limit: usize,
    ) -> Result<Vec<StoredOutboundMetadata>, StorageError> {
        self.call(move |storage| storage.outbound_metadata_page(deferred, limit))
    }

    fn update_outbound_delivery(
        &mut self,
        metadata: &StoredOutboundMetadata,
    ) -> Result<bool, StorageError> {
        let metadata = metadata.clone();
        self.call(move |storage| storage.update_outbound_delivery(&metadata))
    }

    fn remove_outbound_message(&mut self, message_id: &[u8; 32]) -> Result<bool, StorageError> {
        let message_id = *message_id;
        self.call(move |storage| storage.remove_outbound_message(&message_id))
    }

    fn outbound_count(&self, deferred: Option<bool>) -> Result<usize, StorageError> {
        self.call(move |storage| storage.outbound_count(deferred))
    }

    fn upsert_ticket(&mut self, ticket: &crate::ticket::Ticket) -> Result<(), StorageError> {
        let ticket = ticket.clone();
        self.call(move |storage| storage.upsert_ticket(&ticket))
    }
    fn valid_ticket(
        &self,
        destination_hash: &[u8; 16],
        now: f64,
    ) -> Result<Option<crate::ticket::Ticket>, StorageError> {
        let destination_hash = *destination_hash;
        self.call(move |storage| storage.valid_ticket(&destination_hash, now))
    }
    fn remove_tickets(&mut self, destination_hash: &[u8; 16]) -> Result<usize, StorageError> {
        let destination_hash = *destination_hash;
        self.call(move |storage| storage.remove_tickets(&destination_hash))
    }
    fn cull_tickets(&mut self, cutoff: f64) -> Result<usize, StorageError> {
        self.call(move |storage| storage.cull_tickets(cutoff))
    }
    fn upsert_stamp_cost(&mut self, entry: StoredStampCost) -> Result<(), StorageError> {
        self.call(move |storage| storage.upsert_stamp_cost(entry))
    }
    fn stamp_cost(
        &self,
        destination_hash: &[u8; 16],
    ) -> Result<Option<StoredStampCost>, StorageError> {
        let destination_hash = *destination_hash;
        self.call(move |storage| storage.stamp_cost(&destination_hash))
    }
    fn remove_stamp_cost(&mut self, destination_hash: &[u8; 16]) -> Result<bool, StorageError> {
        let destination_hash = *destination_hash;
        self.call(move |storage| storage.remove_stamp_cost(&destination_hash))
    }
    fn cull_stamp_costs_before(&mut self, cutoff: f64) -> Result<usize, StorageError> {
        self.call(move |storage| storage.cull_stamp_costs_before(cutoff))
    }
    fn stamp_cost_count(&self) -> Result<usize, StorageError> {
        self.call(|storage| storage.stamp_cost_count())
    }
    fn upsert_identity(&mut self, identity: StoredIdentity) -> Result<(), StorageError> {
        self.call(move |s| s.upsert_identity(identity))
    }
    fn identity(&self, hash: &[u8; 16]) -> Result<Option<StoredIdentity>, StorageError> {
        let hash = *hash;
        self.call(move |s| s.identity(&hash))
    }
    fn identity_page(&self, limit: usize) -> Result<Vec<StoredIdentity>, StorageError> {
        self.call(move |s| s.identity_page(limit))
    }
    fn upsert_ratchet(&mut self, ratchet: StoredRatchet) -> Result<(), StorageError> {
        self.call(move |s| s.upsert_ratchet(ratchet))
    }
    fn ratchet(&self, hash: &[u8; 16]) -> Result<Option<StoredRatchet>, StorageError> {
        let hash = *hash;
        self.call(move |s| s.ratchet(&hash))
    }
    fn ratchet_page(&self, cutoff: f64, limit: usize) -> Result<Vec<StoredRatchet>, StorageError> {
        self.call(move |s| s.ratchet_page(cutoff, limit))
    }
    fn cull_ratchets_before(&mut self, cutoff: f64) -> Result<usize, StorageError> {
        self.call(move |s| s.cull_ratchets_before(cutoff))
    }
    fn put_state_blob(&mut self, key: &str, value: &[u8]) -> Result<(), StorageError> {
        let key = key.to_string();
        let value = value.to_vec();
        self.call(move |s| s.put_state_blob(&key, &value))
    }
    fn put_state_blobs(&mut self, entries: &[(&str, &[u8])]) -> Result<(), StorageError> {
        let entries = entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_vec()))
            .collect::<Vec<_>>();
        self.call(move |s| {
            let borrowed = entries
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_slice()))
                .collect::<Vec<_>>();
            s.put_state_blobs(&borrowed)
        })
    }
    fn state_blob(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let key = key.to_string();
        self.call(move |s| s.state_blob(&key))
    }
    fn insert_inbound_message(
        &mut self,
        id: [u8; 32],
        at: f64,
        encoded: &[u8],
    ) -> Result<(), StorageError> {
        let encoded = encoded.to_vec();
        self.call(move |s| s.insert_inbound_message(id, at, &encoded))
    }
    fn contains_inbound_message(&self, message_id: &[u8; 32]) -> Result<bool, StorageError> {
        let id = *message_id;
        self.call(move |s| s.contains_inbound_message(&id))
    }
    fn replace_peers(&mut self, peers: &[([u8; 16], Vec<u8>)]) -> Result<(), StorageError> {
        let peers = peers.to_vec();
        self.call(move |s| s.replace_peers(&peers))
    }
    fn peer_page(&self, limit: usize) -> Result<Vec<([u8; 16], Vec<u8>)>, StorageError> {
        self.call(move |s| s.peer_page(limit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStorage;

    #[test]
    fn cloned_handles_share_one_backend() {
        let mut first = spawn_storage_actor(MemoryStorage::new()).unwrap();
        let mut second = first.clone();
        first
            .upsert_transient_id(TransientIdKind::LocallyDelivered, [1; 32], 10)
            .unwrap();
        assert!(
            second
                .contains_transient_id(TransientIdKind::LocallyDelivered, &[1; 32])
                .unwrap()
        );
        second
            .insert_message(&StoredMessage::new(
                [2; 32],
                [3; 32],
                [4; 16],
                20,
                5,
                vec![6],
                false,
            ))
            .unwrap();
        assert_eq!(first.message_store_stats().unwrap().count, 1);
    }

    #[test]
    fn injected_failure_is_counted_and_backend_recovers() {
        let mut storage = spawn_storage_actor(MemoryStorage::new()).unwrap();
        storage.fail_next_operations(2);
        assert!(matches!(
            storage.stamp_cost_count(),
            Err(StorageError::ActorUnavailable)
        ));
        assert!(matches!(
            storage.stamp_cost_count(),
            Err(StorageError::ActorUnavailable)
        ));
        storage
            .upsert_transient_id(TransientIdKind::LocallyDelivered, [7; 32], 10)
            .unwrap();
        assert!(
            storage
                .contains_transient_id(TransientIdKind::LocallyDelivered, &[7; 32])
                .unwrap()
        );
    }
}
