// src/fs_batch.rs -----------------------------------------------------------
use std::{ffi::OsStr, os::windows::prelude::OsStrExt, path::Path,
          time::{Duration, SystemTime}};
use windows::Win32::{
    Foundation::HANDLE,
    Storage::FileSystem::*,
};

#[derive(Clone, Default, Debug)]
pub struct Meta {
    pub size: u64,
    pub created: SystemTime,
    pub modified: SystemTime,
    pub attrs: u32,
}

pub fn scan_dir(path: &Path) -> std::io::Result<Vec<(String, Meta)>> {
    const BUF: usize = 64 * 1024;
    let mut buf = vec![0u8; BUF];

    // 1 Handle aufs Verzeichnis
    let wide: Vec<u16> =
        OsStr::new(path).encode_wide().chain(std::iter::once(0)).collect();
    let dir = unsafe {
        CreateFileW(wide.as_ptr(),
                    FILE_LIST_DIRECTORY,
                    FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE,
                    None, OPEN_EXISTING,
                    FILE_FLAG_BACKUP_SEMANTICS, None)
    };
    if dir == HANDLE::default() { return Err(std::io::Error::last_os_error()); }

    // 2 Blockweises Lesen
    let mut first = true;
    let mut out = Vec::new();
    loop {
        let ok = unsafe {
            GetFileInformationByHandleEx(
                dir,
                if first { FileIdExtdDirectoryRestartInfo }
                      else { FileIdExtdDirectoryInfo },
                buf.as_mut_ptr() as _,
                BUF as u32)
        };
        if !ok.as_bool() { break }

        first = false;
        let mut off = 0;
        while off < BUF {
            let info = unsafe {
                &*(buf[off..].as_ptr() as *const FILE_ID_EXTD_DIR_INFO)
            };
            if info.FileNameLength == 0 { break; }
            // Name
            let utf16 = unsafe {
                std::slice::from_raw_parts(
                    info.FileName.as_ptr(),
                    (info.FileNameLength / 2) as usize)
            };
            let name = String::from_utf16_lossy(utf16);
            if name != "." && name != ".." {
                out.push((name, Meta {
                    size:     info.EndOfFile as u64,
                    created:  filetime(info.CreationTime),
                    modified: filetime(info.LastWriteTime),
                    attrs:    info.FileAttributes,
                }));
            }
            if info.NextEntryOffset == 0 { break; }
            off += info.NextEntryOffset as usize;
        }
    }
    Ok(out)
}

#[inline]
fn filetime(ft: windows::Win32::Foundation::LARGE_INTEGER) -> SystemTime {
    const TICKS:  u64 = 10_000_000;              // 100 ns
    const DELTA:  u64 = 11_644_473_600;          // 1601→1970
    let ticks = ft.QuadPart as u64;
    SystemTime::UNIX_EPOCH +
        Duration::new(ticks / TICKS - DELTA, ((ticks % TICKS) * 100) as u32)
}


// main.rs -------------------------------------------------------------------
mod fs_batch;

use fs_batch::Meta;
use jwalk::{WalkDirGeneric, Parallelism};
use std::{path::Path, sync::Arc, ffi::OsString};

/// (ReadDirState, DirEntryState) ⇒  wir speichern Meta direkt im DirEntry
type CS = ((), Meta);

fn main() -> anyhow::Result<()> {
    let walker = WalkDirGeneric::<CS>::new("C:\\DATA")
        .skip_metadata(true)                             // keine std-Stats
        .parallelism(Parallelism::RayonNew(Some(num_cpus::get())))
        .process_read_dir(|_depth, dir, _state, children| {
            // 1 Batch-Meta einmalig für dieses Verzeichnis holen
            let meta = fs_batch::scan_dir(Path::new(dir))
                        .unwrap_or_default()
                        .into_iter().collect::<std::collections::HashMap<_,_>>();

            // 2 Vorhandene DirEntries anreichern
            for child in children.iter_mut() {
                if let Ok(entry) = child {
                    if let Some(m) =
                        meta.get(&entry.file_name.to_string_lossy().to_string()){
                        entry.client_state = m.clone();
                    }
                }
            }
        });

    for res in walker {
        let entry = res?;
        let m = &entry.client_state;   // Meta liegt jetzt hier
        println!("{:>10}  {:?}  {}", m.size, m.modified, entry.path().display());
    }
    Ok(())
}

