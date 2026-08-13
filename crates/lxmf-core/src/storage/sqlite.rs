use std::path::Path;

use rusqlite::{Connection, OptionalExtension, params};

use super::{LxmfStorage, StorageError, TransientIdKind};
use crate::types::PropagationTransientId;

const SCHEMA_VERSION: u32 = 1;

pub struct SqliteStorage {
    connection: Connection,
}

impl SqliteStorage {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let mut connection = Connection::open(path).map_err(database_error)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(database_error)?;
        connection
            .execute_batch(
                "PRAGMA auto_vacuum=INCREMENTAL;
                 PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=NORMAL;
                 PRAGMA temp_store=FILE;
                 PRAGMA cache_size=-1024;
                 PRAGMA mmap_size=0;
                 PRAGMA wal_autocheckpoint=128;",
            )
            .map_err(database_error)?;
        migrate(&mut connection)?;
        Ok(Self { connection })
    }

    pub fn schema_version(&self) -> Result<u32, StorageError> {
        schema_version(&self.connection)
    }
}

impl LxmfStorage for SqliteStorage {
    fn contains_transient_id(
        &mut self,
        kind: TransientIdKind,
        transient_id: &PropagationTransientId,
    ) -> Result<bool, StorageError> {
        self.connection
            .query_row(
                "SELECT 1 FROM transient_ids WHERE kind = ?1 AND transient_id = ?2",
                params![kind as i64, transient_id.as_slice()],
                |_| Ok(()),
            )
            .optional()
            .map(|row| row.is_some())
            .map_err(database_error)
    }

    fn upsert_transient_id(
        &mut self,
        kind: TransientIdKind,
        transient_id: PropagationTransientId,
        seen_at: i64,
    ) -> Result<(), StorageError> {
        self.connection
            .execute(
                "INSERT INTO transient_ids(transient_id, kind, seen_at) VALUES (?1, ?2, ?3)
                 ON CONFLICT(kind, transient_id) DO UPDATE SET seen_at = excluded.seen_at",
                params![transient_id.as_slice(), kind as i64, seen_at],
            )
            .map(|_| ())
            .map_err(database_error)
    }

    fn upsert_transient_ids(
        &mut self,
        entries: &[(TransientIdKind, PropagationTransientId, i64)],
    ) -> Result<(), StorageError> {
        let transaction = self.connection.transaction().map_err(database_error)?;
        {
            let mut statement = transaction
                .prepare(
                    "INSERT INTO transient_ids(transient_id, kind, seen_at) VALUES (?1, ?2, ?3)
                     ON CONFLICT(kind, transient_id) DO UPDATE SET seen_at = excluded.seen_at",
                )
                .map_err(database_error)?;
            for (kind, transient_id, seen_at) in entries {
                statement
                    .execute(params![transient_id.as_slice(), *kind as i64, *seen_at])
                    .map_err(database_error)?;
            }
        }
        transaction.commit().map_err(database_error)
    }

    fn cull_transient_ids_before(&mut self, cutoff: i64) -> Result<usize, StorageError> {
        self.connection
            .execute("DELETE FROM transient_ids WHERE seen_at < ?1", [cutoff])
            .map_err(database_error)
    }

    fn transient_id_count(&mut self, kind: TransientIdKind) -> Result<usize, StorageError> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM transient_ids WHERE kind = ?1",
                [kind as i64],
                |row| row.get::<_, i64>(0),
            )
            .map_err(database_error)
            .and_then(|count| {
                usize::try_from(count)
                    .map_err(|_| StorageError::InvalidData("negative transient ID count".into()))
            })
    }
}

fn migrate(connection: &mut Connection) -> Result<(), StorageError> {
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_meta (
                 key   TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             ) WITHOUT ROWID;",
        )
        .map_err(database_error)?;

    let found = schema_version(connection)?;
    if found > SCHEMA_VERSION {
        return Err(StorageError::UnsupportedSchema {
            found,
            supported: SCHEMA_VERSION,
        });
    }
    if found < 1 {
        let transaction = connection.transaction().map_err(database_error)?;
        transaction
            .execute_batch(
                "CREATE TABLE transient_ids (
                     transient_id BLOB NOT NULL CHECK(length(transient_id) = 32),
                     kind         INTEGER NOT NULL,
                     seen_at      INTEGER NOT NULL,
                     PRIMARY KEY (kind, transient_id)
                 ) WITHOUT ROWID;
                 CREATE INDEX transient_ids_seen_at ON transient_ids(seen_at);
                 INSERT INTO schema_meta(key, value) VALUES ('schema_version', '1')
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value;",
            )
            .map_err(database_error)?;
        transaction.commit().map_err(database_error)?;
    }
    Ok(())
}

fn schema_version(connection: &Connection) -> Result<u32, StorageError> {
    let value = connection
        .query_row(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(database_error)?;
    value
        .map(|value| {
            value.parse::<u32>().map_err(|_| {
                StorageError::InvalidData(format!("invalid schema version value `{value}`"))
            })
        })
        .transpose()
        .map(|version| version.unwrap_or(0))
}

fn database_error(error: rusqlite::Error) -> StorageError {
    StorageError::Database(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_database_from_newer_schema() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("newer.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE schema_meta (
                     key TEXT PRIMARY KEY,
                     value TEXT NOT NULL
                 ) WITHOUT ROWID;
                 INSERT INTO schema_meta(key, value) VALUES ('schema_version', '999');",
            )
            .unwrap();
        drop(connection);

        let error = SqliteStorage::open(&path)
            .err()
            .expect("newer schema rejected");
        assert!(matches!(
            error,
            StorageError::UnsupportedSchema {
                found: 999,
                supported: SCHEMA_VERSION
            }
        ));
    }
}
