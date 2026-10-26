// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Caller-owned windows retain bounded legacy I/O and fail closed on short reads.
use cellar_ltx::{
    host::{IoActivity, MAX_IO_WINDOW_BYTES},
    DirectFileSystem, FileSystem, HostFile, HostFileIo,
};
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Default)]
struct Calls {
    reads: AtomicUsize,
    writes: AtomicUsize,
}

struct Legacy {
    file: HostFile,
    calls: Arc<Calls>,
    short_read: bool,
    fail_write: bool,
}

impl HostFileIo for Legacy {
    fn read_exact_at(&mut self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        assert!(length <= 65536);
        self.calls.reads.fetch_add(1, Ordering::Relaxed);
        let mut bytes = self.file.read_exact_at(offset, length)?;
        if self.short_read {
            bytes.pop();
        }
        Ok(bytes)
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        assert!(bytes.len() <= 65536);
        self.calls.writes.fetch_add(1, Ordering::Relaxed);
        if self.fail_write {
            self.file.write_all(&bytes[..bytes.len() / 2])?;
            return Err(io::Error::other("uncertain partial write"));
        }
        self.file.write_all(bytes)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.file.file_len()
    }
}

struct NativeWindow(Legacy);

impl HostFileIo for NativeWindow {
    fn read_exact_at(&mut self, _: u64, _: usize) -> io::Result<Vec<u8>> {
        panic!("native window unexpectedly allocated through the legacy reader")
    }

    fn write_all(&mut self, _: &[u8]) -> io::Result<()> {
        panic!("native window unexpectedly used legacy writes")
    }

    fn read_exact_into_at(&mut self, offset: u64, output: &mut [u8]) -> io::Result<()> {
        self.0.calls.reads.fetch_add(1, Ordering::Relaxed);
        self.0.file.read_exact_into_at(offset, output)
    }

    fn write_all_window(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.0.calls.writes.fetch_add(1, Ordering::Relaxed);
        self.0.file.write_all_window(bytes)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.0.file.sync_all()
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.0.file.file_len()
    }
}

fn legacy(calls: Arc<Calls>) -> Legacy {
    Legacy {
        file: DirectFileSystem.temporary_file(None).unwrap(),
        calls,
        short_read: false,
        fail_write: false,
    }
}

#[test]
fn caller_windows_roundtrip_through_legacy_and_native_providers() {
    let bytes: Vec<u8> = (0..MAX_IO_WINDOW_BYTES).map(|i| (i % 251) as u8).collect();
    for native in [false, true] {
        let calls = Arc::<Calls>::default();
        let provider = legacy(calls.clone());
        let mut file = if native {
            HostFile::from_io(NativeWindow(provider))
        } else {
            HostFile::from_io(provider)
        };
        let activity = IoActivity::default();
        file.set_io_activity(Some(activity.clone()));
        file.write_all_window(&bytes).unwrap();
        let mut output = vec![0; bytes.len()];
        file.read_exact_into_at(0, &mut output).unwrap();
        assert_eq!(output, bytes);
        assert_eq!(activity.sequence(), 2);
        let expected = if native { 1 } else { 64 };
        assert_eq!(calls.reads.load(Ordering::Relaxed), expected);
        assert_eq!(calls.writes.load(Ordering::Relaxed), expected);
        file.read_exact_into_at(u64::MAX, &mut []).unwrap();
        file.write_all_window(&[]).unwrap();
        assert_eq!(activity.sequence(), 2);
        assert_eq!(calls.reads.load(Ordering::Relaxed), expected);
        assert_eq!(calls.writes.load(Ordering::Relaxed), expected);
        let mut excessive = vec![0; MAX_IO_WINDOW_BYTES + 1];
        assert_eq!(
            file.read_exact_into_at(0, &mut excessive)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            file.write_all_window(&excessive).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            file.read_exact_into_at(u64::MAX, &mut output[..1])
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(calls.reads.load(Ordering::Relaxed), expected);
        assert_eq!(calls.writes.load(Ordering::Relaxed), expected);
        assert_eq!(activity.sequence(), 2);
    }
}

#[test]
fn failed_windows_do_not_report_completed_activity() {
    let calls = Arc::<Calls>::default();
    let mut provider = legacy(calls.clone());
    provider.file.write_all(&[7; 65536]).unwrap();
    provider.short_read = true;
    provider.fail_write = true;
    let mut file = HostFile::from_io(provider);
    let activity = IoActivity::default();
    file.set_io_activity(Some(activity.clone()));
    assert_eq!(
        file.read_exact_into_at(0, &mut [0; 65536])
            .unwrap_err()
            .kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert!(file.write_all_window(&[8; 65536]).is_err());
    assert_eq!(calls.reads.load(Ordering::Relaxed), 1);
    assert_eq!(calls.writes.load(Ordering::Relaxed), 1);
    assert_eq!(activity.sequence(), 0);
}

#[cfg(unix)]
#[test]
fn native_window_offsets_cross_four_gib_without_allocating_the_sparse_prefix() {
    let mut file = DirectFileSystem.temporary_file(None).unwrap();
    let offset = (1_u64 << 32) + 8192;
    let bytes = vec![43; 65536];
    file.set_len(offset).unwrap();
    file.write_all_at(offset, &bytes).unwrap();
    assert_eq!(file.file_len().unwrap(), offset + bytes.len() as u64);
    let activity = IoActivity::default();
    file.set_io_activity(Some(activity.clone()));
    let mut output = vec![0; bytes.len()];
    file.read_exact_into_at(offset, &mut output).unwrap();
    assert_eq!(output, bytes);
    assert_eq!(activity.sequence(), 1);
    assert_eq!(
        file.read_exact_into_at(offset + 1, &mut output)
            .unwrap_err()
            .kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(
        activity.sequence(),
        1,
        "short native reads reported progress"
    );
}
