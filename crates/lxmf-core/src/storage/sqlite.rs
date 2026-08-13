use std::path::Path;

use rusqlite::{Connection, OptionalExtension, params, params_from_iter, types::Value};

use super::{
    LxmfStorage, MessageStoreStats, StorageError, StoredMessage, StoredMessageMetadata,
    StoredOutboundMessage, StoredOutboundMetadata, TransientIdKind, validate_message,
};
use crate::types::PropagationTransientId;

const SCHEMA_VERSION: u32 = 3;

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
        &self,
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

    fn transient_id_count(&self, kind: TransientIdKind) -> Result<usize, StorageError> {
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

    fn insert_message(&mut self, message: &StoredMessage) -> Result<bool, StorageError> {
        validate_message(message)?;
        let metadata = &message.metadata;
        self.connection
            .execute(
                "INSERT OR IGNORE INTO messages(
                     transient_id, message_hash, destination_hash, stored_at,
                     stamp_value, payload, payload_size, collected, stamped
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    metadata.transient_id.as_slice(),
                    metadata.message_hash.as_slice(),
                    metadata.destination_hash.as_slice(),
                    metadata.stored_at,
                    i64::from(metadata.stamp_value),
                    message.payload,
                    usize_to_i64(metadata.payload_size)?,
                    i64::from(metadata.collected),
                    i64::from(metadata.stamped),
                ],
            )
            .map(|changed| changed == 1)
            .map_err(database_error)
    }

    fn message_metadata(
        &self,
        transient_id: &PropagationTransientId,
    ) -> Result<Option<StoredMessageMetadata>, StorageError> {
        self.connection
            .query_row(
                "SELECT transient_id, message_hash, destination_hash, stored_at,
                        stamp_value, payload_size, collected, stamped
                 FROM messages WHERE transient_id = ?1",
                [transient_id.as_slice()],
                metadata_from_row,
            )
            .optional()
            .map_err(database_error)
    }

    fn message_payload(
        &self,
        transient_id: &PropagationTransientId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.connection
            .query_row(
                "SELECT payload FROM messages WHERE transient_id = ?1",
                [transient_id.as_slice()],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error)
    }

    fn message_metadata_page(
        &self,
        after: Option<&PropagationTransientId>,
        limit: usize,
    ) -> Result<Vec<StoredMessageMetadata>, StorageError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT transient_id, message_hash, destination_hash, stored_at,
                        stamp_value, payload_size, collected, stamped
                 FROM messages
                 WHERE (?1 IS NULL OR transient_id > ?1)
                 ORDER BY transient_id LIMIT ?2",
            )
            .map_err(database_error)?;
        let rows = statement
            .query_map(
                params![after.map(|value| value.as_slice()), usize_to_i64(limit)?],
                metadata_from_row,
            )
            .map_err(database_error)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(database_error)
    }

    fn message_metadata_for_destination(
        &self,
        destination_hash: &[u8; 16],
        after: Option<&PropagationTransientId>,
        limit: usize,
    ) -> Result<Vec<StoredMessageMetadata>, StorageError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT transient_id, message_hash, destination_hash, stored_at,
                        stamp_value, payload_size, collected, stamped
                 FROM messages
                 WHERE destination_hash = ?1 AND (?2 IS NULL OR transient_id > ?2)
                 ORDER BY transient_id LIMIT ?3",
            )
            .map_err(database_error)?;
        let rows = statement
            .query_map(
                params![
                    destination_hash.as_slice(),
                    after.map(|value| value.as_slice()),
                    usize_to_i64(limit)?
                ],
                metadata_from_row,
            )
            .map_err(database_error)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(database_error)
    }

    fn remove_message(
        &mut self,
        transient_id: &PropagationTransientId,
    ) -> Result<bool, StorageError> {
        self.connection
            .execute(
                "DELETE FROM messages WHERE transient_id = ?1",
                [transient_id.as_slice()],
            )
            .map(|changed| changed == 1)
            .map_err(database_error)
    }

    fn set_message_collected(
        &mut self,
        transient_id: &PropagationTransientId,
        collected: bool,
    ) -> Result<bool, StorageError> {
        self.connection
            .execute(
                "UPDATE messages SET collected = ?2 WHERE transient_id = ?1",
                params![transient_id.as_slice(), i64::from(collected)],
            )
            .map(|changed| changed == 1)
            .map_err(database_error)
    }

    fn message_store_stats(&self) -> Result<MessageStoreStats, StorageError> {
        let (count, payload_size) = self
            .connection
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(payload_size), 0) FROM messages",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(database_error)?;
        Ok(MessageStoreStats {
            count: i64_to_usize(count, "message count")?,
            payload_size: i64_to_usize(payload_size, "payload size")?,
        })
    }

    fn remove_messages_stored_before(
        &mut self,
        cutoff: i64,
        limit: usize,
    ) -> Result<usize, StorageError> {
        self.connection
            .execute(
                "DELETE FROM messages WHERE transient_id IN (
                     SELECT transient_id FROM messages
                     WHERE stored_at < ?1
                     ORDER BY stored_at, transient_id LIMIT ?2
                 )",
                params![cutoff, usize_to_i64(limit)?],
            )
            .map_err(database_error)
    }

    fn remove_messages_by_weight(
        &mut self,
        bytes_to_remove: usize,
        now: i64,
        prioritised_destinations: &[[u8; 16]],
        limit: usize,
    ) -> Result<usize, StorageError> {
        if bytes_to_remove == 0 || limit == 0 {
            return Ok(0);
        }
        let transaction = self.connection.transaction().map_err(database_error)?;
        let priority_clause = if prioritised_destinations.is_empty() {
            "0".to_string()
        } else {
            std::iter::repeat_n("?", prioritised_destinations.len())
                .collect::<Vec<_>>()
                .join(", ")
        };
        let query = format!(
            "SELECT transient_id, payload_size FROM messages
             ORDER BY
               (CASE WHEN destination_hash IN ({priority_clause}) THEN 0.1 ELSE 1.0 END) *
               MAX(1.0, (? - stored_at) / 345600.0) * payload_size DESC,
               transient_id
             LIMIT ?"
        );
        let mut parameters = prioritised_destinations
            .iter()
            .map(|destination| Value::Blob(destination.to_vec()))
            .collect::<Vec<_>>();
        parameters.push(Value::Integer(now));
        parameters.push(Value::Integer(usize_to_i64(limit)?));

        let selected = {
            let mut statement = transaction.prepare(&query).map_err(database_error)?;
            let rows = statement
                .query_map(params_from_iter(parameters), |row| {
                    Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(database_error)?;
            let mut selected = Vec::new();
            let mut selected_bytes = 0usize;
            for row in rows {
                if selected_bytes >= bytes_to_remove {
                    break;
                }
                let (transient_id, payload_size) = row.map_err(database_error)?;
                selected_bytes =
                    selected_bytes.saturating_add(i64_to_usize(payload_size, "payload size")?);
                selected.push(transient_id);
            }
            selected
        };
        {
            let mut delete = transaction
                .prepare("DELETE FROM messages WHERE transient_id = ?1")
                .map_err(database_error)?;
            for transient_id in &selected {
                delete.execute([transient_id]).map_err(database_error)?;
            }
        }
        transaction.commit().map_err(database_error)?;
        Ok(selected.len())
    }

    fn upsert_outbound_message(
        &mut self,
        message: &StoredOutboundMessage,
    ) -> Result<(), StorageError> {
        let metadata = &message.metadata;
        self.connection
            .execute(
                "INSERT INTO outbound_messages(
                     message_id, destination_hash, state, delivery_method, deferred,
                     next_delivery_attempt, last_delivery_attempt, delivery_attempts,
                     created_at, progress, encoded_message
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                 ON CONFLICT(message_id) DO UPDATE SET
                     destination_hash = excluded.destination_hash,
                     state = excluded.state,
                     delivery_method = excluded.delivery_method,
                     deferred = excluded.deferred,
                     next_delivery_attempt = excluded.next_delivery_attempt,
                     last_delivery_attempt = excluded.last_delivery_attempt,
                     delivery_attempts = excluded.delivery_attempts,
                     created_at = excluded.created_at,
                     progress = excluded.progress,
                     encoded_message = excluded.encoded_message",
                params![
                    metadata.message_id.as_slice(),
                    metadata.destination_hash.as_slice(),
                    i64::from(metadata.state),
                    i64::from(metadata.delivery_method),
                    i64::from(metadata.deferred),
                    metadata.next_delivery_attempt,
                    metadata.last_delivery_attempt,
                    i64::from(metadata.delivery_attempts),
                    metadata.created_at,
                    metadata.progress,
                    message.encoded_message,
                ],
            )
            .map(|_| ())
            .map_err(database_error)
    }

    fn outbound_message(
        &self,
        message_id: &[u8; 32],
    ) -> Result<Option<StoredOutboundMessage>, StorageError> {
        self.connection
            .query_row(
                "SELECT message_id, destination_hash, state, delivery_method, deferred,
                        next_delivery_attempt, last_delivery_attempt, delivery_attempts,
                        created_at, progress, encoded_message
                 FROM outbound_messages WHERE message_id = ?1",
                [message_id.as_slice()],
                |row| {
                    Ok(StoredOutboundMessage {
                        metadata: outbound_metadata_from_row(row)?,
                        encoded_message: row.get(10)?,
                    })
                },
            )
            .optional()
            .map_err(database_error)
    }

    fn outbound_ready(
        &self,
        now: f64,
        deferred: bool,
        limit: usize,
    ) -> Result<Vec<StoredOutboundMetadata>, StorageError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT message_id, destination_hash, state, delivery_method, deferred,
                        next_delivery_attempt, last_delivery_attempt, delivery_attempts,
                        created_at, progress
                 FROM outbound_messages
                 WHERE deferred = ?1 AND next_delivery_attempt <= ?2
                 ORDER BY next_delivery_attempt, created_at, message_id LIMIT ?3",
            )
            .map_err(database_error)?;
        statement
            .query_map(
                params![i64::from(deferred), now, usize_to_i64(limit)?],
                outbound_metadata_from_row,
            )
            .map_err(database_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error)
    }

    fn outbound_metadata_page(
        &self,
        deferred: bool,
        limit: usize,
    ) -> Result<Vec<StoredOutboundMetadata>, StorageError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT message_id, destination_hash, state, delivery_method, deferred,
                        next_delivery_attempt, last_delivery_attempt, delivery_attempts,
                        created_at, progress
                 FROM outbound_messages WHERE deferred = ?1
                 ORDER BY created_at, message_id LIMIT ?2",
            )
            .map_err(database_error)?;
        statement
            .query_map(
                params![i64::from(deferred), usize_to_i64(limit)?],
                outbound_metadata_from_row,
            )
            .map_err(database_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error)
    }

    fn update_outbound_delivery(
        &mut self,
        metadata: &StoredOutboundMetadata,
    ) -> Result<bool, StorageError> {
        self.connection
            .execute(
                "UPDATE outbound_messages SET
                     destination_hash = ?2, state = ?3, delivery_method = ?4,
                     deferred = ?5, next_delivery_attempt = ?6,
                     last_delivery_attempt = ?7, delivery_attempts = ?8,
                     created_at = ?9, progress = ?10
                 WHERE message_id = ?1",
                params![
                    metadata.message_id.as_slice(),
                    metadata.destination_hash.as_slice(),
                    i64::from(metadata.state),
                    i64::from(metadata.delivery_method),
                    i64::from(metadata.deferred),
                    metadata.next_delivery_attempt,
                    metadata.last_delivery_attempt,
                    i64::from(metadata.delivery_attempts),
                    metadata.created_at,
                    metadata.progress,
                ],
            )
            .map(|changed| changed == 1)
            .map_err(database_error)
    }

    fn remove_outbound_message(&mut self, message_id: &[u8; 32]) -> Result<bool, StorageError> {
        self.connection
            .execute(
                "DELETE FROM outbound_messages WHERE message_id = ?1",
                [message_id.as_slice()],
            )
            .map(|changed| changed == 1)
            .map_err(database_error)
    }

    fn outbound_count(&self, deferred: Option<bool>) -> Result<usize, StorageError> {
        let count = if let Some(deferred) = deferred {
            self.connection
                .query_row(
                    "SELECT COUNT(*) FROM outbound_messages WHERE deferred = ?1",
                    [i64::from(deferred)],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(database_error)?
        } else {
            self.connection
                .query_row("SELECT COUNT(*) FROM outbound_messages", [], |row| {
                    row.get::<_, i64>(0)
                })
                .map_err(database_error)?
        };
        i64_to_usize(count, "outbound count")
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
    if found < 2 {
        let transaction = connection.transaction().map_err(database_error)?;
        transaction
            .execute_batch(
                "CREATE TABLE messages (
                     transient_id     BLOB PRIMARY KEY CHECK(length(transient_id) = 32),
                     message_hash     BLOB NOT NULL CHECK(length(message_hash) = 32),
                     destination_hash BLOB NOT NULL CHECK(length(destination_hash) = 16),
                     stored_at        INTEGER NOT NULL,
                     stamp_value      INTEGER NOT NULL,
                     payload          BLOB NOT NULL,
                     payload_size     INTEGER NOT NULL,
                     collected       INTEGER NOT NULL DEFAULT 0,
                     stamped         INTEGER NOT NULL DEFAULT 0
                 ) WITHOUT ROWID;
                 CREATE INDEX messages_destination
                     ON messages(destination_hash, stored_at);
                 CREATE INDEX messages_stored_at ON messages(stored_at);
                 INSERT INTO schema_meta(key, value) VALUES ('schema_version', '2')
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value;",
            )
            .map_err(database_error)?;
        transaction.commit().map_err(database_error)?;
    }
    if found < 3 {
        let transaction = connection.transaction().map_err(database_error)?;
        transaction
            .execute_batch(
                "CREATE TABLE outbound_messages (
                     message_id            BLOB PRIMARY KEY CHECK(length(message_id) = 32),
                     destination_hash      BLOB NOT NULL CHECK(length(destination_hash) = 16),
                     state                 INTEGER NOT NULL,
                     delivery_method       INTEGER NOT NULL,
                     deferred              INTEGER NOT NULL,
                     next_delivery_attempt REAL NOT NULL,
                     last_delivery_attempt REAL NOT NULL,
                     delivery_attempts     INTEGER NOT NULL,
                     created_at            REAL NOT NULL,
                     progress              REAL NOT NULL,
                     encoded_message       BLOB NOT NULL
                 ) WITHOUT ROWID;
                 CREATE INDEX outbound_ready
                     ON outbound_messages(state, next_delivery_attempt);
                 CREATE INDEX outbound_deferred_ready
                     ON outbound_messages(deferred, next_delivery_attempt);
                 INSERT INTO schema_meta(key, value) VALUES ('schema_version', '3')
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

fn metadata_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredMessageMetadata> {
    let transient_id = fixed_blob::<32>(row.get(0)?, 0)?;
    let message_hash = fixed_blob::<32>(row.get(1)?, 1)?;
    let destination_hash = fixed_blob::<16>(row.get(2)?, 2)?;
    let stamp_value = u16::try_from(row.get::<_, i64>(4)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            4,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })?;
    let payload_size_value = row.get::<_, i64>(5)?;
    let payload_size = usize::try_from(payload_size_value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            5,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })?;
    Ok(StoredMessageMetadata {
        transient_id,
        message_hash,
        destination_hash,
        stored_at: row.get(3)?,
        stamp_value,
        payload_size,
        collected: row.get::<_, i64>(6)? != 0,
        stamped: row.get::<_, i64>(7)? != 0,
    })
}

fn outbound_metadata_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredOutboundMetadata> {
    let state = u8::try_from(row.get::<_, i64>(2)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })?;
    let delivery_method = u8::try_from(row.get::<_, i64>(3)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })?;
    let delivery_attempts = u32::try_from(row.get::<_, i64>(7)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            7,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })?;
    Ok(StoredOutboundMetadata {
        message_id: fixed_blob::<32>(row.get(0)?, 0)?,
        destination_hash: fixed_blob::<16>(row.get(1)?, 1)?,
        state,
        delivery_method,
        deferred: row.get::<_, i64>(4)? != 0,
        next_delivery_attempt: row.get(5)?,
        last_delivery_attempt: row.get(6)?,
        delivery_attempts,
        created_at: row.get(8)?,
        progress: row.get(9)?,
    })
}

fn fixed_blob<const N: usize>(value: Vec<u8>, column: usize) -> rusqlite::Result<[u8; N]> {
    value.try_into().map_err(|value: Vec<u8>| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Blob,
            format!("expected {N}-byte blob, got {} bytes", value.len()).into(),
        )
    })
}

fn usize_to_i64(value: usize) -> Result<i64, StorageError> {
    i64::try_from(value).map_err(|_| StorageError::InvalidData("value exceeds i64".into()))
}

fn i64_to_usize(value: i64, name: &str) -> Result<usize, StorageError> {
    usize::try_from(value)
        .map_err(|_| StorageError::InvalidData(format!("invalid {name}: {value}")))
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

    #[test]
    fn migrates_v1_database_to_current_schema() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("v1.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE schema_meta (
                     key TEXT PRIMARY KEY,
                     value TEXT NOT NULL
                 ) WITHOUT ROWID;
                 CREATE TABLE transient_ids (
                     transient_id BLOB NOT NULL CHECK(length(transient_id) = 32),
                     kind INTEGER NOT NULL,
                     seen_at INTEGER NOT NULL,
                     PRIMARY KEY (kind, transient_id)
                 ) WITHOUT ROWID;
                 CREATE INDEX transient_ids_seen_at ON transient_ids(seen_at);
                 INSERT INTO schema_meta(key, value) VALUES ('schema_version', '1');",
            )
            .unwrap();
        drop(connection);

        let mut storage = SqliteStorage::open(&path).unwrap();
        assert_eq!(storage.schema_version().unwrap(), 3);
        assert_eq!(storage.message_store_stats().unwrap().count, 0);
        assert!(
            storage
                .insert_message(&StoredMessage::new(
                    [1; 32],
                    [2; 32],
                    [3; 16],
                    10,
                    4,
                    vec![5],
                    false,
                ))
                .unwrap()
        );
    }

    #[test]
    fn creates_outbound_table_and_ready_index() {
        let directory = tempfile::tempdir().unwrap();
        let storage = SqliteStorage::open(&directory.path().join("outbound.sqlite")).unwrap();

        storage
            .connection
            .execute(
                "INSERT INTO outbound_messages(
                     message_id, destination_hash, state, delivery_method, deferred,
                     next_delivery_attempt, last_delivery_attempt, delivery_attempts,
                     created_at, progress, encoded_message
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    [0x11_u8; 32].as_slice(),
                    [0x22_u8; 16].as_slice(),
                    1_i64,
                    2_i64,
                    0_i64,
                    100.0_f64,
                    0.0_f64,
                    0_i64,
                    50.0_f64,
                    0.0_f64,
                    [0x33_u8; 8].as_slice(),
                ],
            )
            .unwrap();

        let index_sql: String = storage
            .connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'outbound_ready'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(index_sql.contains("state, next_delivery_attempt"));
    }

    #[test]
    fn interrupted_message_insert_leaves_no_partial_row() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("interrupted.sqlite");
        drop(SqliteStorage::open(&path).unwrap());

        let mut connection = Connection::open(&path).unwrap();
        let transaction = connection.transaction().unwrap();
        transaction
            .execute(
                "INSERT INTO messages(
                     transient_id, message_hash, destination_hash, stored_at,
                     stamp_value, payload, payload_size, collected, stamped
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    [0x11_u8; 32].as_slice(),
                    [0x22_u8; 32].as_slice(),
                    [0x33_u8; 16].as_slice(),
                    100_i64,
                    0_i64,
                    [0x44_u8; 64].as_slice(),
                    64_i64,
                    0_i64,
                    0_i64,
                ],
            )
            .unwrap();
        // Simulate interruption before COMMIT. Dropping the transaction must
        // roll it back, leaving neither metadata nor payload visible.
        drop(transaction);
        drop(connection);

        let mut storage = SqliteStorage::open(&path).unwrap();
        assert_eq!(
            storage.message_store_stats().unwrap(),
            MessageStoreStats::default()
        );
        assert!(storage.message_metadata(&[0x11; 32]).unwrap().is_none());
        assert!(storage.message_payload(&[0x11; 32]).unwrap().is_none());

        assert!(
            storage
                .insert_message(&StoredMessage::new(
                    [0x11; 32],
                    [0x22; 32],
                    [0x33; 16],
                    100,
                    0,
                    vec![0x44; 64],
                    false,
                ))
                .unwrap()
        );
    }

    #[test]
    fn applies_low_memory_pragmas() {
        let directory = tempfile::tempdir().unwrap();
        let storage = SqliteStorage::open(&directory.path().join("pragmas.sqlite")).unwrap();
        let text = |name: &str| {
            storage
                .connection
                .pragma_query_value(None, name, |row| row.get::<_, String>(0))
                .unwrap()
        };
        let integer = |name: &str| {
            storage
                .connection
                .pragma_query_value(None, name, |row| row.get::<_, i64>(0))
                .unwrap()
        };
        assert_eq!(text("journal_mode"), "wal");
        assert_eq!(integer("synchronous"), 1);
        assert_eq!(integer("temp_store"), 1);
        assert_eq!(integer("cache_size"), -1024);
        assert_eq!(integer("mmap_size"), 0);
        assert_eq!(integer("wal_autocheckpoint"), 128);
        assert_eq!(integer("busy_timeout"), 5000);
        assert_eq!(integer("auto_vacuum"), 2);
    }
}
