//! Where a detector writes its files: ophyd-async `_path_providers.py`.
//!
//! A [`PathProvider`] is asked once per staging for a [`PathInfo`] (the
//! directory, the filename without extension, how many directory levels the
//! IOC may create, and the URI a reader uses for that directory). A
//! [`FilenameProvider`] names the file inside it.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::Local;
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

/// `base_directory_uri or generate_directory_uri(base_directory_path)`,
/// ending in `/`.
fn base_directory_uri(uri: Option<&str>, base_directory_path: &Path) -> String {
    let mut uri = uri.map_or_else(
        || generate_directory_uri(base_directory_path),
        str::to_string,
    );
    if !uri.ends_with('/') {
        uri.push('/');
    }
    uri
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
        let base_uri = base_directory_uri(
            self.base_directory_uri.as_deref(),
            &self.base_directory_path,
        );
        PathInfo::new(
            self.base_directory_path.join(&dir_name),
            filename,
            self.create_dir_depth,
            Some(format!("{base_uri}{dir_name}")),
        )
    }
}

/// A `{year}/{month}/{day}` directory for today's local date under a base
/// directory (ophyd-async `YMDPathProvider`). A caller's data key goes before
/// the date, or after it with
/// [`with_datakey_name_as_base_dir`](Self::with_datakey_name_as_base_dir).
pub struct YmdPathProvider {
    filename_provider: Arc<dyn FilenameProvider>,
    base_directory_path: PathBuf,
    base_directory_uri: Option<String>,
    create_dir_depth: i64,
    datakey_name_as_base_dir: bool,
}

impl YmdPathProvider {
    /// Files named by `filename_provider` in today's directory under
    /// `base_directory_path`; the writer may create the three date levels.
    pub fn new(
        filename_provider: Arc<dyn FilenameProvider>,
        base_directory_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            filename_provider,
            base_directory_path: pure_path(base_directory_path.into()),
            base_directory_uri: None,
            create_dir_depth: -3,
            datakey_name_as_base_dir: false,
        }
    }

    /// URI a reader resolves the base directory by; the date directories
    /// are appended to it. An empty one derives it from the path.
    pub fn with_base_directory_uri(mut self, base_directory_uri: impl Into<String>) -> Self {
        self.base_directory_uri = Some(base_directory_uri.into()).filter(|uri| !uri.is_empty());
        self
    }

    /// Missing directory levels the writer may create (see [`PathInfo::new`]).
    pub fn with_create_dir_depth(mut self, create_dir_depth: i64) -> Self {
        self.create_dir_depth = create_dir_depth;
        self
    }

    /// Put the data key's directory under the date instead of above it.
    pub fn with_datakey_name_as_base_dir(mut self, datakey_name_as_base_dir: bool) -> Self {
        self.datakey_name_as_base_dir = datakey_name_as_base_dir;
        self
    }
}

impl PathProvider for YmdPathProvider {
    fn path_info(&self, datakey_name: Option<&str>) -> Result<PathInfo> {
        let date = Local::now().format("%Y/%m/%d").to_string();
        let ymd_dir = match datakey_name {
            None => PathBuf::from(date),
            Some(key) if self.datakey_name_as_base_dir => pure_path(Path::new(&date).join(key)),
            Some(key) => pure_path(Path::new(key).join(date)),
        };
        let filename = self.filename_provider.filename(datakey_name)?;
        let base_uri = base_directory_uri(
            self.base_directory_uri.as_deref(),
            &self.base_directory_path,
        );
        PathInfo::new(
            self.base_directory_path.join(&ymd_dir),
            filename,
            self.create_dir_depth,
            Some(format!("{base_uri}{}", ymd_dir.display())),
        )
    }
}

fn io_error(path: &Path, err: std::io::Error) -> BsrsError {
    BsrsError::Backend(format!("{}: {err}", path.display()))
}

/// The highest `N` of the `{N}_*` directories in `path`, `None` when there
/// is none.
fn highest_numbered_dir(path: &Path) -> Result<Option<u64>> {
    let mut highest = None;
    for entry in std::fs::read_dir(path).map_err(|e| io_error(path, e))? {
        let entry = entry.map_err(|e| io_error(path, e))?;
        if !entry.path().is_dir() {
            continue;
        }
        let number = entry
            .file_name()
            .to_str()
            .and_then(|name| name.split_once('_'))
            .map(|(digits, _)| digits)
            .filter(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|digits| digits.parse::<u64>().ok());
        highest = highest.max(number);
    }
    Ok(highest)
}

/// Whether `name` is a `YYYY-MM-DD` directory name.
fn is_date_dir_name(name: &str) -> bool {
    let b = name.as_bytes();
    b.len() == 10
        && b.iter().enumerate().all(|(i, c)| match i {
            4 | 7 => *c == b'-',
            _ => c.is_ascii_digit(),
        })
}

/// A `{counter}_{filename}/` directory under the base provider's directory,
/// numbered one past the highest `{N}_` directory already there
/// (ophyd-async `AutoMaxIncrementingPathProvider`). With
/// [`with_dated`](Self::with_dated) it goes in today's `YYYY-MM-DD`
/// directory, numbered past every other day's directories too.
pub struct AutoMaxIncrementingPathProvider {
    base_path_provider: Arc<dyn PathProvider>,
    max_digits: usize,
    dated: bool,
    next_value: Mutex<u64>,
}

impl AutoMaxIncrementingPathProvider {
    /// Numbered directories in `base_path_provider`'s directory, named after
    /// its filename; `base_path_provider` should not increment itself.
    pub fn new(base_path_provider: Arc<dyn PathProvider>) -> Self {
        Self {
            base_path_provider,
            max_digits: 4,
            dated: false,
            next_value: Mutex::new(0),
        }
    }

    /// Pad the counter to `max_digits`.
    pub fn with_max_digits(mut self, max_digits: usize) -> Self {
        self.max_digits = max_digits;
        self
    }

    /// Counter value used while no numbered directory exists.
    pub fn with_starting_value(self, starting_value: u64) -> Self {
        *self.next_value.lock().unwrap() = starting_value;
        self
    }

    /// Number directories inside a directory for today's date.
    pub fn with_dated(mut self, dated: bool) -> Self {
        self.dated = dated;
        self
    }
}

impl PathProvider for AutoMaxIncrementingPathProvider {
    fn path_info(&self, _datakey_name: Option<&str>) -> Result<PathInfo> {
        let base = self.base_path_provider.path_info(None)?;
        let base_dir = base.directory_path();
        let mut next_value = self.next_value.lock().unwrap();
        let path = if self.dated {
            for entry in std::fs::read_dir(base_dir).map_err(|e| io_error(base_dir, e))? {
                let entry = entry.map_err(|e| io_error(base_dir, e))?;
                if entry.file_name().to_str().is_some_and(is_date_dir_name) {
                    if let Some(highest) = highest_numbered_dir(&entry.path())? {
                        *next_value = (*next_value).max(highest + 1);
                    }
                }
            }
            base_dir.join(Local::now().format("%Y-%m-%d").to_string())
        } else {
            base_dir.to_path_buf()
        };
        // Upstream reuses the highest number when it equals the stored next
        // value, which reopens an existing directory; take the one after it.
        let value = if path.exists() {
            highest_numbered_dir(&path)?.map_or(*next_value, |highest| highest + 1)
        } else {
            *next_value
        };
        *next_value = value + 1;
        let filename = base.filename();
        let full_path = pure_path(
            path.join(format!(
                "{value:0width$}_{}",
                filename.trim_matches('_'),
                width = self.max_digits
            ))
            .join(filename.trim_end_matches('_')),
        );
        let (Some(directory_path), Some(filename)) = (full_path.parent(), full_path.file_name())
        else {
            return Err(BsrsError::InvalidValue(format!(
                "no file name in {}",
                full_path.display()
            )));
        };
        PathInfo::new(directory_path, filename.to_string_lossy(), 0, None)
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

    fn today(format: &str) -> String {
        Local::now().format(format).to_string()
    }

    /// Creates the directory and the file, as the upstream test's `write_file`.
    fn write_file(info: &PathInfo) {
        std::fs::create_dir_all(info.directory_path()).unwrap();
        std::fs::write(info.directory_path().join(info.filename()), b"").unwrap();
    }

    #[test]
    fn auto_max_increment_path_provider() {
        let tmp = tempfile::tempdir().unwrap();
        let base: Arc<dyn PathProvider> = Arc::new(StaticPathProvider::new(
            Arc::new(StaticFilenameProvider::new("capture")),
            tmp.path(),
        ));

        let pp = AutoMaxIncrementingPathProvider::new(base.clone()).with_max_digits(3);
        let info = pp.path_info(None).unwrap();
        write_file(&info);
        assert_eq!(info.directory_path(), tmp.path().join("000_capture"));
        assert_eq!(info.filename(), "capture");
        assert_eq!(info.create_dir_depth(), 0);
        assert_eq!(
            info.directory_uri(),
            format!("file://localhost{}/000_capture/", tmp.path().display())
        );
        let info = pp.path_info(None).unwrap();
        write_file(&info);
        assert_eq!(info.directory_path(), tmp.path().join("001_capture"));

        // A fresh provider continues past the directories already there.
        let pp = AutoMaxIncrementingPathProvider::new(base.clone());
        let info = pp.path_info(None).unwrap();
        write_file(&info);
        assert_eq!(info.directory_path(), tmp.path().join("0002_capture"));

        let pp = AutoMaxIncrementingPathProvider::new(base).with_dated(true);
        let day = tmp.path().join(today("%Y-%m-%d"));
        let info = pp.path_info(None).unwrap();
        write_file(&info);
        assert_eq!(info.directory_path(), day.join("0000_capture"));
        let info = pp.path_info(None).unwrap();
        write_file(&info);
        assert_eq!(info.directory_path(), day.join("0001_capture"));
    }

    /// Upstream returns `0000_capture` again here: its highest number equals
    /// the fresh provider's next value.
    #[test]
    fn auto_max_increment_path_provider_never_reopens_the_highest_directory() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir(tmp.path().join("0000_capture")).unwrap();
        let pp = AutoMaxIncrementingPathProvider::new(Arc::new(StaticPathProvider::new(
            Arc::new(StaticFilenameProvider::new("capture")),
            tmp.path(),
        )));
        assert_eq!(
            pp.path_info(None).unwrap().directory_path(),
            tmp.path().join("0001_capture")
        );
    }

    #[test]
    fn auto_max_increment_path_provider_dated_numbers_past_other_days() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("2000-01-01/0007_capture")).unwrap();
        std::fs::create_dir_all(tmp.path().join("2000-01-02")).unwrap();
        std::fs::create_dir_all(tmp.path().join("0042_capture")).unwrap();
        let pp = AutoMaxIncrementingPathProvider::new(Arc::new(StaticPathProvider::new(
            Arc::new(StaticFilenameProvider::new("_capture_")),
            tmp.path(),
        )))
        .with_dated(true);
        let info = pp.path_info(None).unwrap();
        assert_eq!(
            info.directory_path(),
            tmp.path().join(today("%Y-%m-%d")).join("0008_capture")
        );
        assert_eq!(info.filename(), "_capture");
    }

    #[test]
    fn ymd_path_provider() {
        let tmp = tempfile::tempdir().unwrap();
        let date = today("%Y/%m/%d");
        let pp = YmdPathProvider::new(static_filename(), tmp.path());

        let info = pp.path_info(None).unwrap();
        assert_eq!(info.directory_path(), tmp.path().join(&date));
        assert_eq!(info.create_dir_depth(), -3);
        assert_eq!(
            info.directory_uri(),
            format!("file://localhost{}/{date}/", tmp.path().display())
        );

        let info = pp.path_info(Some("test_device")).unwrap();
        assert_eq!(
            info.directory_path(),
            tmp.path().join("test_device").join(&date)
        );

        let pp = YmdPathProvider::new(static_filename(), "/data")
            .with_base_directory_uri("http://host/data")
            .with_datakey_name_as_base_dir(true);
        let info = pp.path_info(Some("test_device")).unwrap();
        assert_eq!(
            info.directory_path(),
            Path::new("/data").join(&date).join("test_device")
        );
        assert_eq!(
            info.directory_uri(),
            format!("http://host/data/{date}/test_device/")
        );
    }
}
