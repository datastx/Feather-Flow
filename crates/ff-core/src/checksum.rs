//! SHA-256 checksum utility for change detection.

use sha2::{Digest, Sha256};

/// Compute SHA256 checksum of a string
pub fn compute_checksum(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    let result = hasher.finalize();
    format!("{:x}", result)
}
