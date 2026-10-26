// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A pending or failed file operation cannot sustain a publication hint.
#![allow(clippy::disallowed_methods)]

use cellar_ltx::{host::IoActivity, DirectFileSystem, FileSystem, HostFile, HostFileIo};
use std::{io, sync::mpsc};

struct HeldRead {
    file: HostFile,
    entered: mpsc::SyncSender<()>,
    release: mpsc::Receiver<bool>,
}

impl HostFileIo for HeldRead {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)
    }

    fn read_exact_at(&mut self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        self.entered.send(()).map_err(io::Error::other)?;
        if !self.release.recv().map_err(io::Error::other)? {
            return Err(io::Error::other("injected failed read"));
        }
        self.file.read_exact_at(offset, length)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.file.file_len()
    }
}

#[test]
fn activity_advances_after_completed_io_but_not_while_pending_or_after_failure() {
    for succeed in [false, true] {
        let mut source = DirectFileSystem.temporary_file(None).unwrap();
        source.write_all(b"retained").unwrap();
        let (entered, ready) = mpsc::sync_channel(1);
        let (release, resumed) = mpsc::sync_channel(1);
        let activity = IoActivity::default();
        let mut file = HostFile::from_io(HeldRead {
            file: source,
            entered,
            release: resumed,
        });
        file.set_io_activity(Some(activity.clone()));
        let worker = std::thread::spawn(move || {
            let result = file.read_exact_at(0, 8);
            (file, result)
        });
        let observed = ready.recv_timeout(std::time::Duration::from_secs(5));
        let pending_sequence = activity.sequence();
        release.send(succeed).unwrap();
        let (mut file, result) = worker.join().unwrap();
        observed.unwrap();
        assert_eq!(pending_sequence, 0);
        if succeed {
            assert_eq!(result.unwrap(), b"retained");
        } else {
            assert!(result.is_err());
        }
        assert_eq!(activity.sequence(), u64::from(succeed));
        let before = activity.sequence();
        file.file_len().unwrap();
        assert_eq!(
            activity.sequence(),
            before,
            "metadata inspection faked data progress"
        );
        file.sync_all().unwrap();
        assert_eq!(activity.sequence(), before + 1);
    }
}
