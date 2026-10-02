//! Cheap helpers for runtime measurements.

use std::path::Path;
use std::time::Instant;

use sha2::{Digest, Sha256};

use crate::errors::IngestError;

/// Stream-compute SHA-256 of a file. Memory use is bounded by the buffer
/// size (currently 64 KiB).
pub fn sha256_of_file(path: &Path) -> Result<String, IngestError> {
    use std::fs::File;
    use std::io::Read;

    let f = File::open(path).map_err(|e| IngestError::io(path, e))?;
    let mut reader = std::io::BufReader::new(f);
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|e| IngestError::io(path, e))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Stopwatch wrapper.
#[derive(Debug, Clone)]
pub struct Timer {
    start: Instant,
}

impl Timer {
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }
    pub fn elapsed(&self) -> std::time::Duration {
        self.start.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_known_short_string() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.txt");
        std::fs::write(&p, b"abc").unwrap();
        let h = sha256_of_file(&p).unwrap();
        // sha256("abc")
        assert_eq!(
            h,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
