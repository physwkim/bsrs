//! Where a detector writes its files: ophyd-async `_path_providers.py`.
//!
//! A [`PathProvider`] is asked once per staging for a [`PathInfo`] (the
//! directory, the filename without extension, how many directory levels the
//! IOC may create, and the URI a reader uses for that directory). A
//! [`FilenameProvider`] names the file inside it.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use crate::core::error::{BsrsError, Result};

/// `path` as a `PurePath` holds it: no trailing separator, no `.` or
/// repeated separators.
fn pure_path(path: PathBuf) -> PathBuf {
    path.components().collect()
}

/// `file://localhost{directory_path}/` (ophyd-async `generate_directory_uri`).
fn generate_directory_uri(directory_path: &Path) -> String {
    format!("file://localhost{}/", directory_path.display())
}

/// Whether `uri` has a scheme, a network location and a path, as
/// `urllib.parse.urlparse` splits them.
fn has_scheme_netloc_path(uri: &str) -> bool {
    let Some((scheme, rest)) = uri.split_once(':') else {
        return false;
    };
    let scheme_ok = scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    let Some(rest) = rest.strip_prefix("//") else {
        return false;
    };
    let netloc_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let tail = &rest[netloc_end..];
    let path = &tail[..tail.find(['?', '#']).unwrap_or(tail.len())];
    scheme_ok && netloc_end > 0 && !path.is_empty()
}

/// Where and how to write a file (ophyd-async `PathInfo`). Only
/// [`PathInfo::new`] builds one, so `directory_path` is always absolute and
/// `directory_uri` always ends in `/` with a scheme, host and path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathInfo {
    directory_path: PathBuf,
    filename: String,
    create_dir_depth: i64,
    directory_uri: String,
}

impl PathInfo {
    /// `directory_path` must be absolute. `create_dir_depth` is the
    /// `CreateDirectory` index after which missing levels of the split
    /// directory path are created (negative counts from the end, `0` creates
    /// nothing missing below the root). `directory_uri` is where a reader
    /// finds the directory; `None` derives `file://localhost{directory_path}/`.
    pub fn new(
        directory_path: impl Into<PathBuf>,
        filename: impl Into<String>,
        create_dir_depth: i64,
        directory_uri: Option<String>,
    ) -> Result<Self> {
        let directory_path = pure_path(directory_path.into());
        if !directory_path.is_absolute() {
            return Err(BsrsError::InvalidValue(format!(
                "directory_path must be an absolute path, got {}",
                directory_path.display()
            )));
        }
        let directory_uri = match directory_uri {
            None => generate_directory_uri(&directory_path),
            Some(mut uri) => {
                if !uri.ends_with('/') {
                    uri.push('/');
                }
                if !has_scheme_netloc_path(&uri) {
                    return Err(BsrsError::InvalidValue(format!(
                        "Directory URI must include a scheme, netlocation, and path. Got: {uri}"
                    )));
                }
                uri
            }
        };
        Ok(Self {
            directory_path,
            filename: filename.into(),
            create_dir_depth,
            directory_uri,
        })
    }

    /// Directory the files go into, on the host that writes them.
    pub fn directory_path(&self) -> &Path {
        &self.directory_path
    }

    /// Base filename, without extension.
    pub fn filename(&self) -> &str {
        &self.filename
    }

    /// Missing directory levels the writer may create (see [`PathInfo::new`]).
    pub fn create_dir_depth(&self) -> i64 {
        self.create_dir_depth
    }

    /// URI a reader resolves the directory by, ending in `/`.
    pub fn directory_uri(&self) -> &str {
        &self.directory_uri
    }
}

/// Names the files a detector writes, without extension (ophyd-async
/// `FilenameProvider`).
pub trait FilenameProvider: Send + Sync {
    /// The filename for the next staging of the detector writing
    /// `datakey_name`.
    fn filename(&self, datakey_name: Option<&str>) -> Result<String>;
}

/// Tells a detector where to write its data (ophyd-async `PathProvider`).
pub trait PathProvider: Send + Sync {
    /// Where the next staging of the detector writing `datakey_name` writes.
    fn path_info(&self, datakey_name: Option<&str>) -> Result<PathInfo>;
}

/// The same filename on every call (ophyd-async `StaticFilenameProvider`).
#[derive(Clone, Debug)]
pub struct StaticFilenameProvider {
    filename: String,
}

impl StaticFilenameProvider {
    /// Always name files `filename`.
    pub fn new(filename: impl Into<String>) -> Self {
        Self {
            filename: filename.into(),
        }
    }
}

impl FilenameProvider for StaticFilenameProvider {
    fn filename(&self, _datakey_name: Option<&str>) -> Result<String> {
        Ok(self.filename.clone())
    }
}

/// A fresh UUID as each filename (ophyd-async `UUIDFilenameProvider`).
pub struct UuidFilenameProvider {
    uuid_call: Box<dyn Fn() -> Uuid + Send + Sync>,
}

impl UuidFilenameProvider {
    /// Name each file by what `uuid_call` returns: ophyd-async's
    /// `uuid_call_func` with its `uuid_call_args` bound, e.g.
    /// `|| Uuid::new_v5(&Uuid::NAMESPACE_URL, b"test")` with uuid's `v5`
    /// feature. [`Default`] is `Uuid::new_v4`.
    pub fn new(uuid_call: impl Fn() -> Uuid + Send + Sync + 'static) -> Self {
        Self {
            uuid_call: Box::new(uuid_call),
        }
    }
}

impl Default for UuidFilenameProvider {
    fn default() -> Self {
        Self::new(Uuid::new_v4)
    }
}

impl FilenameProvider for UuidFilenameProvider {
    fn filename(&self, _datakey_name: Option<&str>) -> Result<String> {
        Ok((self.uuid_call)().to_string())
    }
}

/// `{base_filename}{delimiter}{counter}`, the counter zero-padded and advanced
/// on every call (ophyd-async `AutoIncrementFilenameProvider`).
pub struct AutoIncrementFilenameProvider {
    base_filename: String,
    max_digits: usize,
    increment: u64,
    inc_delimiter: String,
    current_value: Mutex<u64>,
}

impl AutoIncrementFilenameProvider {
    /// Start at `{base_filename}_00000`, advancing by 1.
    pub fn new(base_filename: impl Into<String>) -> Self {
        Self {
            base_filename: base_filename.into(),
            max_digits: 5,
            increment: 1,
            inc_delimiter: "_".to_string(),
            current_value: Mutex::new(0),
        }
    }

    /// Pad the counter to `max_digits`; a counter longer than that fails.
    pub fn with_max_digits(mut self, max_digits: usize) -> Self {
        self.max_digits = max_digits;
        self
    }

    /// First counter value.
    pub fn with_starting_value(self, starting_value: u64) -> Self {
        *self.current_value.lock().unwrap() = starting_value;
        self
    }

    /// Counter step per call.
    pub fn with_increment(mut self, increment: u64) -> Self {
        self.increment = increment;
        self
    }

    /// Text between the base filename and the counter (ophyd-async
    /// `inc_delimeter`).
    pub fn with_inc_delimiter(mut self, inc_delimiter: impl Into<String>) -> Self {
        self.inc_delimiter = inc_delimiter.into();
        self
    }
}

impl FilenameProvider for AutoIncrementFilenameProvider {
    fn filename(&self, _datakey_name: Option<&str>) -> Result<String> {
        let mut current = self.current_value.lock().unwrap();
        if current.to_string().len() > self.max_digits {
            return Err(BsrsError::InvalidValue(format!(
                "Auto incrementing filename counter exceeded maximum of {} digits!",
                self.max_digits
            )));
        }
        let filename = format!(
            "{}{}{:0width$}",
            self.base_filename,
            self.inc_delimiter,
            *current,
            width = self.max_digits
        );
        *current += self.increment;
        Ok(filename)
    }
}

/// Every file in one directory (ophyd-async `StaticPathProvider`).
pub struct StaticPathProvider {
    filename_provider: Arc<dyn FilenameProvider>,
    directory_path: PathBuf,
    directory_uri: Option<String>,
    create_dir_depth: i64,
}

impl StaticPathProvider {
    /// Files named by `filename_provider` in `directory_path`, which must be
    /// absolute; the URI is derived from it and no directory is created.
    pub fn new(
        filename_provider: Arc<dyn FilenameProvider>,
        directory_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            filename_provider,
            directory_path: pure_path(directory_path.into()),
            directory_uri: None,
            create_dir_depth: 0,
        }
    }

    /// URI a reader resolves the directory by, when it differs from where
    /// the writer sees it; an empty one derives it from the path.
    pub fn with_directory_uri(mut self, directory_uri: impl Into<String>) -> Self {
        self.directory_uri = Some(directory_uri.into()).filter(|uri| !uri.is_empty());
        self
    }

    /// Missing directory levels the writer may create (see [`PathInfo::new`]).
    pub fn with_create_dir_depth(mut self, create_dir_depth: i64) -> Self {
        self.create_dir_depth = create_dir_depth;
        self
    }
}

impl PathProvider for StaticPathProvider {
    fn path_info(&self, datakey_name: Option<&str>) -> Result<PathInfo> {
        let filename = self.filename_provider.filename(datakey_name)?;
        PathInfo::new(
            self.directory_path.clone(),
            filename,
            self.create_dir_depth,
            self.directory_uri.clone(),
        )
    }
}

/// Counter state of an [`AutoIncrementingPathProvider`].
struct DirCounter {
    current_value: u64,
    calls_since_inc: u64,
}

/// A new numbered directory under a base directory every
/// `num_calls_per_inc` calls (ophyd-async `AutoIncrementingPathProvider`).
/// The directory is `{base_name}{delimiter}{counter}`, else
/// `{datakey_name}{delimiter}{counter}` when the caller names a data key,
/// else the bare zero-padded counter.
pub struct AutoIncrementingPathProvider {
    filename_provider: Arc<dyn FilenameProvider>,
    base_directory_path: PathBuf,
    base_directory_uri: Option<String>,
    create_dir_depth: i64,
    max_digits: usize,
    num_calls_per_inc: u64,
    increment: u64,
    inc_delimiter: String,
    base_name: Option<String>,
    counter: Mutex<DirCounter>,
}

impl AutoIncrementingPathProvider {
    /// Numbered directories from `00000` under `base_directory_path`, one per
    /// call, files named by `filename_provider`.
    pub fn new(
        filename_provider: Arc<dyn FilenameProvider>,
        base_directory_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            filename_provider,
            base_directory_path: pure_path(base_directory_path.into()),
            base_directory_uri: None,
            create_dir_depth: 0,
            max_digits: 5,
            num_calls_per_inc: 1,
            increment: 1,
            inc_delimiter: "_".to_string(),
            base_name: None,
            counter: Mutex::new(DirCounter {
                current_value: 0,
                calls_since_inc: 0,
            }),
        }
    }

    /// URI a reader resolves the base directory by; each numbered directory
    /// is appended to it. An empty one derives it from the path.
    pub fn with_base_directory_uri(mut self, base_directory_uri: impl Into<String>) -> Self {
        self.base_directory_uri = Some(base_directory_uri.into()).filter(|uri| !uri.is_empty());
        self
    }

    /// Missing directory levels the writer may create (see [`PathInfo::new`]).
    pub fn with_create_dir_depth(mut self, create_dir_depth: i64) -> Self {
        self.create_dir_depth = create_dir_depth;
        self
    }

    /// Pad the counter to `max_digits`.
    pub fn with_max_digits(mut self, max_digits: usize) -> Self {
        self.max_digits = max_digits;
        self
    }

    /// First counter value.
    pub fn with_starting_value(self, starting_value: u64) -> Self {
        self.counter.lock().unwrap().current_value = starting_value;
        self
    }

    /// Calls that share one directory before the counter advances.
    pub fn with_num_calls_per_inc(mut self, num_calls_per_inc: u64) -> Self {
        self.num_calls_per_inc = num_calls_per_inc;
        self
    }

    /// Counter step.
    pub fn with_increment(mut self, increment: u64) -> Self {
        self.increment = increment;
        self
    }

    /// Text between the directory's base name and the counter (ophyd-async
    /// `inc_delimeter`).
    pub fn with_inc_delimiter(mut self, inc_delimiter: impl Into<String>) -> Self {
        self.inc_delimiter = inc_delimiter.into();
        self
    }

    /// Prefix every directory with `base_name` instead of the data key.
    pub fn with_base_name(mut self, base_name: impl Into<String>) -> Self {
        self.base_name = Some(base_name.into());
        self
    }
}

impl PathProvider for AutoIncrementingPathProvider {
    fn path_info(&self, datakey_name: Option<&str>) -> Result<PathInfo> {
        let filename = self.filename_provider.filename(datakey_name)?;
        let dir_name = {
            let mut counter = self.counter.lock().unwrap();
            let padded = format!("{:0width$}", counter.current_value, width = self.max_digits);
            counter.calls_since_inc += 1;
            if counter.calls_since_inc == self.num_calls_per_inc {
                counter.calls_since_inc = 0;
                counter.current_value += self.increment;
            }
            match (&self.base_name, datakey_name) {
                (Some(base), _) => format!("{base}{}{padded}", self.inc_delimiter),
                (None, Some(key)) => format!("{key}{}{padded}", self.inc_delimiter),
                (None, None) => padded,
            }
        };
        let mut base_uri = self
            .base_directory_uri
            .clone()
            .unwrap_or_else(|| generate_directory_uri(&self.base_directory_path));
        if !base_uri.ends_with('/') {
            base_uri.push('/');
        }
        PathInfo::new(
            self.base_directory_path.join(&dir_name),
            filename,
            self.create_dir_depth,
            Some(format!("{base_uri}{dir_name}")),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn static_filename() -> Arc<dyn FilenameProvider> {
        Arc::new(StaticFilenameProvider::new("test-file"))
    }

    // The cases below port ophyd-async tests/unit_tests/core/test_providers.py.

    #[test]
    fn path_info_invalid_directory_path() {
        let err = PathInfo::new("relative/path", "file.txt", 0, None).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid value: directory_path must be an absolute path, got relative/path"
        );
    }

    #[test]
    fn path_info_invalid_directory_uri() {
        let err = PathInfo::new("/tmp", "file.txt", 0, Some("not_a_valid_uri".into())).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid value: Directory URI must include a scheme, netlocation, and path. \
             Got: not_a_valid_uri/"
        );
        // urlparse finds no network location in a single-slash file URI.
        assert!(PathInfo::new("/tmp", "f", 0, Some("file:/tmp/".into())).is_err());
        assert!(PathInfo::new("/tmp", "f", 0, Some("file:///tmp/".into())).is_err());
    }

    #[test]
    fn auto_increment_filename_provider() {
        let fp = AutoIncrementFilenameProvider::new("")
            .with_inc_delimiter("")
            .with_max_digits(3);
        let dp = StaticPathProvider::new(Arc::new(fp), "/tmp");
        for i in 0..1000 {
            let info = dp.path_info(None).unwrap();
            assert_eq!(info.filename(), format!("{i:03}"));
        }
        let err = dp.path_info(None).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid value: Auto incrementing filename counter exceeded maximum of 3 digits!"
        );
    }

    #[test]
    fn auto_increment_filename_provider_pads_after_the_delimiter() {
        let fp = AutoIncrementFilenameProvider::new("scan")
            .with_starting_value(7)
            .with_increment(5);
        assert_eq!(fp.filename(None).unwrap(), "scan_00007");
        assert_eq!(fp.filename(None).unwrap(), "scan_00012");
    }

    #[test]
    fn uuid_filename_provider() {
        let dp = StaticPathProvider::new(Arc::new(UuidFilenameProvider::default()), "/tmp");
        let info = dp.path_info(None).unwrap();
        let parsed = Uuid::parse_str(info.filename()).unwrap();
        assert_eq!(parsed.get_version_num(), 4);
        assert_eq!(info.filename(), parsed.hyphenated().to_string());

        let fixed = Uuid::from_u128(0x6ba7_b810_9dad_11d1_80b4_00c0_4fd4_30c8);
        let dp =
            StaticPathProvider::new(Arc::new(UuidFilenameProvider::new(move || fixed)), "/tmp");
        assert_eq!(
            dp.path_info(None).unwrap().filename(),
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
        );
    }

    #[test]
    fn auto_increment_path_provider() {
        let pp = AutoIncrementingPathProvider::new(static_filename(), "/tmp/base")
            .with_num_calls_per_inc(3)
            .with_increment(2);
        for _ in 0..3 {
            let info = pp.path_info(None).unwrap();
            assert_eq!(info.directory_path(), Path::new("/tmp/base/00000"));
        }
        let info = pp.path_info(None).unwrap();
        assert_eq!(info.directory_path(), Path::new("/tmp/base/00002"));
        assert_eq!(info.directory_uri(), "file://localhost/tmp/base/00002/");
        assert_eq!(info.filename(), "test-file");
    }

    #[test]
    fn auto_increment_path_provider_names_the_directory_by_base_name_else_data_key() {
        let pp = AutoIncrementingPathProvider::new(static_filename(), "/tmp/base")
            .with_base_directory_uri("http://host/files");
        let info = pp.path_info(Some("det")).unwrap();
        assert_eq!(info.directory_path(), Path::new("/tmp/base/det_00000"));
        assert_eq!(info.directory_uri(), "http://host/files/det_00000/");
        let pp = AutoIncrementingPathProvider::new(static_filename(), "/tmp/base")
            .with_base_name("run")
            .with_inc_delimiter("-")
            .with_max_digits(2);
        let info = pp.path_info(Some("det")).unwrap();
        assert_eq!(info.directory_path(), Path::new("/tmp/base/run-00"));
    }

    #[test]
    fn path_provider_fails_non_absolute_path() {
        let pp = StaticPathProvider::new(static_filename(), "non_absolute_path");
        let err = pp.path_info(None).unwrap_err();
        assert!(
            err.to_string()
                .contains("directory_path must be an absolute path"),
            "{err}"
        );
    }

    #[test]
    fn path_provider_with_different_uri() {
        let pp = StaticPathProvider::new(static_filename(), "/tmp/posix_path_a")
            .with_directory_uri("file://localhost/tmp/posix_path_b/");
        let info = pp.path_info(None).unwrap();
        assert_eq!(info.directory_path(), Path::new("/tmp/posix_path_a"));
        assert_eq!(info.directory_uri(), "file://localhost/tmp/posix_path_b/");
    }

    #[test]
    fn static_path_provider_derives_the_uri_and_keeps_the_depth() {
        let pp =
            StaticPathProvider::new(static_filename(), "/data//scans/").with_create_dir_depth(-2);
        let info = pp.path_info(Some("det")).unwrap();
        assert_eq!(info.directory_path().to_str(), Some("/data/scans"));
        assert_eq!(info.directory_uri(), "file://localhost/data/scans/");
        assert_eq!(info.create_dir_depth(), -2);
        let pp = StaticPathProvider::new(static_filename(), "/data")
            .with_directory_uri("http://host/data");
        assert_eq!(
            pp.path_info(None).unwrap().directory_uri(),
            "http://host/data/"
        );
        // `directory_uri or generate_directory_uri(...)`: empty derives it.
        let pp = StaticPathProvider::new(static_filename(), "/data").with_directory_uri("");
        assert_eq!(
            pp.path_info(None).unwrap().directory_uri(),
            "file://localhost/data/"
        );
        let pp = AutoIncrementingPathProvider::new(static_filename(), "/data")
            .with_base_directory_uri("");
        assert_eq!(
            pp.path_info(None).unwrap().directory_uri(),
            "file://localhost/data/00000/"
        );
    }
}
