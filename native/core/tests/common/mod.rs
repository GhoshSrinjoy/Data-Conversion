//! Helpers shared by the integration tests.

use std::path::{Path, PathBuf};

/// A directory for one test in the system's temporary directory. It is removed when
/// the test passes; a failing test leaves its files behind to look at.
pub struct TestDir(PathBuf);

impl TestDir {
    pub fn new(name: &str) -> TestDir {
        let d = std::env::temp_dir().join(format!("dcv-test-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        TestDir(d)
    }
}

impl std::ops::Deref for TestDir {
    type Target = PathBuf;

    fn deref(&self) -> &PathBuf {
        &self.0
    }
}

impl AsRef<Path> for TestDir {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
