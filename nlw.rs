// fast_jwalk.rs
//
// High‑performance recursive directory traversal on Windows using
// jwalk's parallel scheduler but feeding metadata from Win32
// FindFirstFileExW with FIND_FIRST_EX_LARGE_FETCH.
//
// Add the following to your Cargo.toml:
//
// [dependencies]
// jwalk = "0.8"
// windows = { version = "0.56", features = [
//     "Win32_Foundation",
//     "Win32_Storage_FileSystem"
// ] }
// anyhow = "1"   # optional, for error handling in your own main
//
// This file exposes `walk_with_basic_info()` which returns a jwalk iterator.
// Its `DirEntry::client_state` contains a `BasicInfo` struct with size,
// creation and modification timestamps – without ever opening the files.

use std::{
    collections::HashMap,
    ffi::OsString,
    os::windows::ffi::OsStringExt,
    path::{Path, PathBuf},
};

use jwalk::{DirEntry, Error, WalkDirGeneric};
use windows::{
    core::PCWSTR,
    Win32::Storage::FileSystem::{
        FindClose, FindFirstFileExW, FindNextFileW, FINDEX_INFO_LEVELS,
        FINDEX_SEARCH_OPS, FIND_FIRST_EX_LARGE_FETCH, WIN32_FIND_DATAW,
    },
};

/// Basic metadata extracted straight from `WIN32_FIND_DATAW`.
#[derive(Clone, Debug, Default)]
pub struct BasicInfo {
    /// File size in bytes.
    pub size: u64,
    /// Creation timestamp as raw FILETIME (100‑ns ticks since 1601‑01‑01 UTC).
    pub created: u64,
    /// Last‑write timestamp as raw FILETIME (100‑ns ticks since 1601‑01‑01 UTC).
    pub modified: u64,
}

/// Collect a map "file name -> BasicInfo" for all items in `dir` using a single
/// `FindFirstFileExW` call with `FIND_FIRST_EX_LARGE_FETCH`.
unsafe fn list_dir_infos(dir: &Path) -> windows::core::Result<HashMap<OsString, BasicInfo>> {
    let mut infos = HashMap::new();

    // Build wide‑string search pattern "dir\\*\0".
    let mut pattern: Vec<u16> = dir
        .as_os_str()
        .encode_wide()
        .chain(Some(b'\\' as u16))
        .chain(Some(b'*' as u16))
        .chain(Some(0))
        .collect();

    let mut data = WIN32_FIND_DATAW::default();
    let handle = FindFirstFileExW(
        PCWSTR(pattern.as_ptr()),
        FINDEX_INFO_LEVELS::FindExInfoBasic,
        &mut data,
        FINDEX_SEARCH_OPS::FindExSearchNameMatch,
        None,
        FIND_FIRST_EX_LARGE_FETCH,
    );
    if handle.is_invalid() {
        return Err(windows::core::Error::from_win32());
    }

    loop {
        push_info(&mut infos, &data);
        if FindNextFileW(handle, &mut data) == false.into() {
            break;
        }
    }
    FindClose(handle);
    Ok(infos)
}

fn push_info(map: &mut HashMap<OsString, BasicInfo>, data: &WIN32_FIND_DATAW) {
    unsafe {
        let mut len = 0usize;
        while len < data.cFileName.len() && data.cFileName[len] != 0 {
            len += 1;
        }
        if len == 0 {
            return;
        }
        let name = OsString::from_wide(&data.cFileName[..len]);
        if name == OsString::from(".") || name == OsString::from("..") {
            return;
        }
        let size = ((data.nFileSizeHigh as u64) << 32) | (data.nFileSizeLow as u64);
        let created = ((data.ftCreationTime.dwHighDateTime as u64) << 32)
            | data.ftCreationTime.dwLowDateTime as u64;
        let modified = ((data.ftLastWriteTime.dwHighDateTime as u64) << 32)
            | data.ftLastWriteTime.dwLowDateTime as u64;

        map.insert(
            name,
            BasicInfo {
                size,
                created,
                modified,
            },
        );
    }
}

/// Returns a jwalk iterator in which `DirEntry::client_state` is populated with
/// the `BasicInfo` gathered via Win32 API – completely handle‑free.
pub fn walk_with_basic_info<P: AsRef<Path>>(
    root: P,
) -> impl Iterator<Item = Result<DirEntry<BasicInfo>, Error>> {
    WalkDirGeneric::<BasicInfo>::new(root).process_read_dir(|_depth, dir_path, _state, children| {
        if let Ok(map) = unsafe { list_dir_infos(dir_path) } {
            for child in children.iter_mut() {
                if let Ok(entry) = child {
                    if let Some(info) = map.get(&entry.file_name) {
                        entry.client_state = info.clone();
                    }
                }
            }
        }
    })
}

// --- Optional CLI demo (enable with `cargo test -- --nocapture` on Windows) ---
#[cfg(test)]
mod cli_example {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn filetime_to_unix(ft: u64) -> SystemTime {
        // FILETIME epochs start 1601‑01‑01; UNIX starts 1970‑01‑01.
        const EPOCH_DIFF: u64 = 116444736000000000; // 100‑ns ticks
        let nanos = (ft - EPOCH_DIFF) * 100;
        UNIX_EPOCH + Duration::from_nanos(nanos)
    }

    #[test]
    fn demo() {
        // Replace with your own path, e.g. r"\\\\server\\share".
        let root = PathBuf::from(r"C:\\Windows\\System32");
        for entry in walk_with_basic_info(root).flatten() {
            if entry.file_type().is_file() {
                let bi = &entry.client_state;
                println!(
                    "{},{},created={:?},modified={:?}",
                    entry.path().display(),
                    bi.size,
                    filetime_to_unix(bi.created),
                    filetime_to_unix(bi.modified)
                );
            }
        }
    }
}
