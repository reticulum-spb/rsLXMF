use std::sync::{Arc, Mutex, mpsc};

use super::{
    LxmfStorage, MessageStoreStats, StorageError, StoredMessage, StoredMessageMetadata,
    StoredOutboundMessage, StoredOutboundMetadata, TransientIdKind,
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
}

impl ActorInner {
    fn new(sender: mpsc::Sender<StorageOperation>, worker: std::thread::JoinHandle<()>) -> Self {
        Self {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(Some(worker)),
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
}
