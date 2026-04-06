use durable_streams_client::Offset;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct OffsetStore {
    path: PathBuf,
    state: Mutex<StoredOffsets>,
    writer: Mutex<()>,
}

impl OffsetStore {
    /// Open a file-backed offset store.
    ///
    /// # Errors
    ///
    /// Returns an error when the file exists but cannot be read or parsed.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, OffsetStoreError> {
        let path = path.as_ref().to_path_buf();
        let state = match tokio::fs::read_to_string(&path).await {
            Ok(raw) => serde_json::from_str(&raw).map_err(|source| OffsetStoreError::Parse {
                path: path.clone(),
                source,
            })?,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                StoredOffsets::default()
            }
            Err(source) => {
                return Err(OffsetStoreError::Read {
                    path: path.clone(),
                    source,
                });
            }
        };
        Ok(Self {
            path,
            state: Mutex::new(state),
            writer: Mutex::new(()),
        })
    }

    pub async fn load(&self, stream_path: &str) -> Option<Offset> {
        let state = self.state.lock().await;
        state.streams.get(stream_path).cloned().map(Offset::from)
    }

    /// Persist the latest acknowledged offset for a stream.
    ///
    /// # Errors
    ///
    /// Returns an error when the offset file cannot be serialized or written.
    pub async fn save(&self, stream_path: &str, offset: &Offset) -> Result<(), OffsetStoreError> {
        {
            let mut state = self.state.lock().await;
            state
                .streams
                .insert(stream_path.to_string(), offset.as_str().to_string());
        }

        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|source| {
                OffsetStoreError::CreateDir {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }
        let _write_guard = self.writer.lock().await;
        let serialized = {
            let state = self.state.lock().await;
            serde_json::to_vec_pretty(&*state).map_err(OffsetStoreError::Serialize)?
        };
        write_atomic(&self.path, &serialized).await?;
        Ok(())
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), OffsetStoreError> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("offsets.json");
    let temp_path = path.with_file_name(format!("{file_name}.tmp"));
    tokio::fs::write(&temp_path, bytes)
        .await
        .map_err(|source| OffsetStoreError::Write {
            path: temp_path.clone(),
            source,
        })?;
    tokio::fs::rename(&temp_path, path)
        .await
        .map_err(|source| OffsetStoreError::Rename {
            from: temp_path,
            to: path.to_path_buf(),
            source,
        })?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StoredOffsets {
    streams: HashMap<String, String>,
}

#[derive(Debug, thiserror::Error)]
pub enum OffsetStoreError {
    #[error("failed to read offset store `{path}`")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse offset store `{path}`")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to create directory `{path}`")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write offset store `{path}`")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to rename offset store from `{from}` to `{to}`")]
    Rename {
        from: PathBuf,
        to: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize offset store")]
    Serialize(#[source] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::OffsetStore;
    use durable_streams_client::Offset;
    use std::path::PathBuf;

    struct TestFile(PathBuf);

    impl TestFile {
        fn new(prefix: &str) -> Self {
            Self(std::env::temp_dir().join(format!(
                "{prefix}-{}-{}.json",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )))
        }

        fn path(&self) -> &PathBuf {
            &self.0
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
            let temp_path = self.0.with_file_name(format!(
                "{}.tmp",
                self.0
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("offsets.json")
            ));
            let _ = std::fs::remove_file(temp_path);
        }
    }

    #[tokio::test]
    async fn round_trips_saved_offsets() {
        let path = TestFile::new("durable-streams-kafka-bridge-offsets");
        let store = OffsetStore::open(path.path()).await.unwrap();
        store
            .save("/v1/stream/orders", &Offset::from("o42"))
            .await
            .unwrap();

        let reopened = OffsetStore::open(path.path()).await.unwrap();
        assert_eq!(
            reopened.load("/v1/stream/orders").await,
            Some(Offset::from("o42"))
        );
    }
}
