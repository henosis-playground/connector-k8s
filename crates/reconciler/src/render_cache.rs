//! Atomic, bounded local storage for deterministic renderer results.

use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;

use tempfile::Builder;

#[derive(Clone, Debug)]
pub(crate) struct RenderCache {
    root: PathBuf,
    max_entries: usize,
    lock: Arc<Mutex<()>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CacheLookup {
    Hit,
    Miss,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CacheStore {
    pub(crate) evicted: usize,
}

impl RenderCache {
    pub(crate) fn new(root: PathBuf, max_entries: usize) -> Self {
        Self {
            root,
            max_entries,
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.max_entries > 0
    }

    pub(crate) fn restore(&self, key: &str, destination: &Path) -> io::Result<CacheLookup> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| io::Error::other("render cache lock is poisoned"))?;
        let source = self.entry(key).join("rendered");
        if !source.try_exists()? {
            return Ok(CacheLookup::Miss);
        }
        let expected_digest = fs::read_to_string(self.entry(key).join("tree.blake3"))?;
        let actual_digest = tree_digest(&source)?;
        if expected_digest.trim() != actual_digest {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "render cache content digest does not match",
            ));
        }
        copy_tree(&source, destination)?;
        Ok(CacheLookup::Hit)
    }

    pub(crate) fn store(&self, key: &str, source: &Path) -> io::Result<CacheStore> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| io::Error::other("render cache lock is poisoned"))?;
        fs::create_dir_all(&self.root)?;
        let entry = self.entry(key);
        if entry.try_exists()? && !entry.join("rendered").try_exists()? {
            fs::remove_dir_all(&entry)?;
        }
        if !entry.try_exists()? {
            let staging = Builder::new().prefix(".staging-").tempdir_in(&self.root)?;
            let rendered = staging.path().join("rendered");
            fs::create_dir_all(&rendered)?;
            copy_tree(source, &rendered)?;
            fs::write(staging.path().join("tree.blake3"), tree_digest(&rendered)?)?;
            let staging = staging.keep();
            if let Err(error) = fs::rename(&staging, &entry) {
                let raced = entry.try_exists().unwrap_or(false);
                let _ = fs::remove_dir_all(&staging);
                if !raced {
                    return Err(error);
                }
            }
        }
        Ok(CacheStore {
            evicted: self.evict()?,
        })
    }

    pub(crate) fn invalidate(&self, key: &str) -> io::Result<()> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| io::Error::other("render cache lock is poisoned"))?;
        let entry = self.entry(key);
        if entry.try_exists()? {
            fs::remove_dir_all(entry)?;
        }
        Ok(())
    }

    fn entry(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    fn evict(&self) -> io::Result<usize> {
        let directory = fs::read_dir(&self.root)?.filter_map(|entry| entry.ok());
        for staging in
            directory.filter(|entry| entry.file_name().to_string_lossy().starts_with(".staging-"))
        {
            fs::remove_dir_all(staging.path())?;
        }
        let mut entries = fs::read_dir(&self.root)?
            .filter_map(Result::ok)
            .filter(|entry| is_recipe_key(&entry.file_name().to_string_lossy()))
            .filter_map(|entry| {
                let metadata = entry.metadata().ok()?;
                metadata.is_dir().then(|| {
                    (
                        metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                        entry.path(),
                    )
                })
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        let excess = entries.len().saturating_sub(self.max_entries);
        for (_, entry) in entries.into_iter().take(excess) {
            fs::remove_dir_all(entry)?;
        }
        Ok(excess)
    }
}

fn is_recipe_key(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn copy_tree(source: &Path, destination: &Path) -> io::Result<()> {
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            fs::create_dir_all(&target)?;
            copy_tree(&entry.path(), &target)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), target)?;
        } else {
            return Err(io::Error::other(
                "render cache does not accept symlinks or special files",
            ));
        }
    }
    Ok(())
}

pub(crate) fn tree_digest(root: &Path) -> io::Result<String> {
    let mut entries = Vec::new();
    collect_entries(root, root, &mut entries)?;
    entries.sort();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"henosis.dev/k8s-render-tree/v1\0");
    for relative in entries {
        let path = root.join(&relative);
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            return Err(io::Error::other(
                "render cache does not accept symlinks or special files",
            ));
        }
        hash_framed(&mut hasher, relative.as_os_str().as_bytes());
        hasher.update(&metadata.permissions().mode().to_be_bytes());
        if metadata.is_dir() {
            hasher.update(b"directory\0");
        } else if metadata.is_file() {
            hasher.update(b"file\0");
            hash_framed(&mut hasher, &fs::read(path)?);
        } else {
            return Err(io::Error::other(
                "render cache does not accept symlinks or special files",
            ));
        }
    }
    Ok(hex::encode(hasher.finalize().as_bytes()))
}

fn collect_entries(root: &Path, directory: &Path, entries: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        entries.push(
            path.strip_prefix(root)
                .map_err(io::Error::other)?
                .to_owned(),
        );
        if entry.file_type()?.is_dir() {
            collect_entries(root, &path, entries)?;
        }
    }
    Ok(())
}

fn hash_framed(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_restores_a_complete_tree() {
        let root = tempfile::tempdir().unwrap();
        let source = tempfile::tempdir().unwrap();
        fs::create_dir_all(source.path().join("nested")).unwrap();
        fs::write(source.path().join("manifest.json"), b"manifest").unwrap();
        fs::write(source.path().join("nested/resource.yaml"), b"resource").unwrap();
        let cache = RenderCache::new(root.path().into(), 2);
        let key = "a".repeat(64);

        assert_eq!(cache.store(&key, source.path()).unwrap().evicted, 0);
        let destination = tempfile::tempdir().unwrap();
        assert_eq!(
            cache.restore(&key, destination.path()).unwrap(),
            CacheLookup::Hit
        );
        assert_eq!(
            fs::read(destination.path().join("nested/resource.yaml")).unwrap(),
            b"resource"
        );
    }

    #[test]
    fn entry_count_is_bounded_by_fifo_eviction() {
        let root = tempfile::tempdir().unwrap();
        let source = tempfile::tempdir().unwrap();
        fs::write(source.path().join("manifest.json"), b"manifest").unwrap();
        let cache = RenderCache::new(root.path().into(), 2);

        cache.store(&"a".repeat(64), source.path()).unwrap();
        cache.store(&"b".repeat(64), source.path()).unwrap();
        assert_eq!(
            cache.store(&"c".repeat(64), source.path()).unwrap().evicted,
            1
        );
        let entries = fs::read_dir(root.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| is_recipe_key(&entry.file_name().to_string_lossy()))
            .count();
        assert_eq!(entries, 2);
    }

    #[test]
    fn corrupted_content_is_rejected_before_restore() {
        let root = tempfile::tempdir().unwrap();
        let source = tempfile::tempdir().unwrap();
        fs::write(source.path().join("manifest.json"), b"original").unwrap();
        let cache = RenderCache::new(root.path().into(), 1);
        let key = "a".repeat(64);
        cache.store(&key, source.path()).unwrap();
        fs::write(
            root.path().join(&key).join("rendered/manifest.json"),
            b"corrupt",
        )
        .unwrap();

        let destination = tempfile::tempdir().unwrap();
        let error = cache.restore(&key, destination.path()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(fs::read_dir(destination.path()).unwrap().next().is_none());
    }
}
