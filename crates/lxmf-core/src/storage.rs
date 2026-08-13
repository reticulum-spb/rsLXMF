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
    UnsupportedSchema { found: u32, supported: u32 },
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "storage I/O error: {error}"),
            Self::Database(error) => write!(formatter, "storage database error: {error}"),
            Self::InvalidData(error) => write!(formatter, "invalid storage data: {error}"),
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

/// Synchronous because the router/storage actor owns each implementation.
pub trait LxmfStorage: Send {
    fn contains_transient_id(
        &mut self,
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

    fn transient_id_count(&mut self, kind: TransientIdKind) -> Result<usize, StorageError>;
}

#[derive(Debug, Default)]
pub struct MemoryStorage {
    transient_ids: HashMap<(TransientIdKind, PropagationTransientId), i64>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LxmfStorage for MemoryStorage {
    fn contains_transient_id(
        &mut self,
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

    fn transient_id_count(&mut self, kind: TransientIdKind) -> Result<usize, StorageError> {
        Ok(self
            .transient_ids
            .keys()
            .filter(|(entry_kind, _)| *entry_kind == kind)
            .count())
    }
}

#[cfg(feature = "sqlite")]
mod sqlite;

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

    #[test]
    fn memory_storage_contract() {
        transient_contract(&mut MemoryStorage::new());
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_storage_contract() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lxmf.sqlite");
        let mut storage = SqliteStorage::open(&path).unwrap();
        transient_contract(&mut storage);

        storage
            .upsert_transient_id(TransientIdKind::LocallyDelivered, [0x44; 32], 300)
            .unwrap();
        drop(storage);
        let mut reopened = SqliteStorage::open(&path).unwrap();
        assert!(
            reopened
                .contains_transient_id(TransientIdKind::LocallyDelivered, &[0x44; 32])
                .unwrap()
        );
        assert_eq!(reopened.schema_version().unwrap(), 1);
    }
}
