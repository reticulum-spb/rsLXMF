//! Atomic file output used by explicit message export APIs.
//!
//! Durable router and daemon state belongs to [`crate::storage`] and SQLite;
//! this module does not implement a parallel file-backed state store.

use std::fs;
use std::io;
use std::path::Path;

/// Atomically write `data` to `path`: unique tmp file + flush + fsync + rename.
///
/// Tmp name is `<path>.tmp.<pid>.<16 random hex>` so concurrent writers (or
/// other processes) never collide; the tmp file is unlinked on error. An fsync
/// failure is logged as a warning and is non-fatal, matching Python
/// `LXMessage.write_to_directory` — LXMessage.py:674-696 (1.0.1).
pub fn write_file_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    use std::io::Write;

    let random: [u8; 8] = rand::random();
    let tmp_name = format!(
        "{}.tmp.{}.{}",
        path.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
        std::process::id(),
        hex::encode(random)
    );
    let tmp = path.with_file_name(tmp_name);

    let result = (|| {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(data)?;
        file.flush()?;
        if let Err(e) = file.sync_all() {
            tracing::warn!(
                "Error while waiting for persist fsync for {}: {e}",
                tmp.display()
            );
        }
        drop(file);
        fs::rename(&tmp, path)
    })();

    if result.is_err()
        && tmp.exists()
        && let Err(e) = fs::remove_file(&tmp)
    {
        tracing::error!(
            "Error while cleaning temporary file {} for {}: {e}",
            tmp.display(),
            path.display()
        );
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn write_file_atomic_writes_and_replaces_without_leftover_tmp() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("message");

        write_file_atomic(&path, b"first").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"first");

        // Replaces existing content (os.replace semantics — LXMessage.py:687).
        write_file_atomic(&path, b"second").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"second");

        // No .tmp.* residue in the directory after successful writes.
        let leftovers: Vec<_> = fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(leftovers.is_empty());
    }

    #[test]
    fn write_file_atomic_cleans_tmp_on_error() {
        let tmp = TempDir::new().unwrap();
        // Destination is a directory: the final rename fails, and the tmp
        // file must be unlinked (LXMessage.py:691-693).
        let dir_path = tmp.path().join("collide");
        fs::create_dir(&dir_path).unwrap();

        assert!(write_file_atomic(&dir_path, b"data").is_err());
        let leftovers: Vec<_> = fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(leftovers.is_empty());
    }
}
