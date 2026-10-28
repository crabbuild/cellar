// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Concurrent readers must share a verified receive and its physical charge.

use super::*;
use std::{sync::mpsc, thread, time::Duration};

const DEADLINE: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug)]
enum First {
    Success,
    IoFailure,
    Corrupt,
    Panic,
}

struct GatedSource {
    source: Arc<Source>,
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    calls: AtomicUsize,
    first: First,
}

impl ExtentSource for GatedSource {
    fn read_into(&self, id: ExtentId, download: &mut ExtentDownload) -> Result<(), Error> {
        if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv_timeout(DEADLINE).unwrap();
            match self.first {
                First::Success => {}
                First::IoFailure | First::Panic => {
                    download.append(0, &[0; 65536])?;
                    assert!(
                        !matches!(self.first, First::Panic),
                        "injected receive panic"
                    );
                    return Err(io::Error::from(io::ErrorKind::ConnectionReset).into());
                }
                First::Corrupt => {
                    for offset in (0..u64::from(id.length())).step_by(65536) {
                        let count = (u64::from(id.length()) - offset).min(65536) as usize;
                        download.append(offset, &vec![0; count])?;
                    }
                    return Ok(());
                }
            }
        }
        self.source.read_into(id, download)
    }
}

struct Gate {
    image: Arc<ExtentImage>,
    source: Arc<GatedSource>,
    entered: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
}

impl Gate {
    fn new(f: &Fixture, scratch: ScratchPool, first: First) -> Self {
        let (entered, source_entered) = mpsc::channel();
        let (release, source_release) = mpsc::channel();
        let source = Arc::new(GatedSource {
            source: f.source.clone(),
            entered,
            release: Mutex::new(source_release),
            calls: AtomicUsize::new(0),
            first,
        });
        let image = Arc::new(f.image_with(source.clone(), scratch));
        *f.source.probe.lock().unwrap() = Some(Arc::downgrade(&image));
        Self {
            image,
            source,
            entered: source_entered,
            release,
        }
    }
}

#[test]
fn concurrent_same_extent_reads_share_one_receive_even_during_cache_eviction() {
    let f = Fixture::new(8, 4096);
    let scratch = ScratchPool::new(ScratchLimits {
        max_bytes: EXTENT_BYTES,
        max_files: 1,
    })
    .unwrap();
    let Gate {
        image,
        source,
        entered: source_entered,
        release,
    } = Gate::new(&f, scratch.clone(), First::Success);
    let leader = {
        let image = image.clone();
        thread::spawn(move || image.read_exact_at(0, 100))
    };
    source_entered.recv_timeout(DEADLINE).unwrap();
    let (started, waiting) = mpsc::channel();
    let (finished, outcomes) = mpsc::channel();
    let mut waiters = Vec::new();
    for _ in 0..2 {
        let reader = image.clone();
        let started = started.clone();
        let finished = finished.clone();
        waiters.push(thread::spawn(move || {
            started.send(()).unwrap();
            finished.send(reader.read_exact_at(0, 100)).unwrap();
        }));
        waiting.recv_timeout(DEADLINE).unwrap();
        // Clearing residency must not forget or refund a live receive.
        image.evict_all().unwrap();
    }
    let premature = outcomes.recv_timeout(Duration::from_millis(200));
    assert_eq!(
        scratch.usage().unwrap(),
        ScratchUsage {
            reserved_bytes: EXTENT_BYTES,
            reserved_files: 1,
        }
    );
    // Release and join every producer before checking the expected regression.
    release.send(()).unwrap();
    let leader_result = leader.join().unwrap();
    for waiter in waiters {
        waiter.join().unwrap();
    }
    drop(finished);
    let mut results: Vec<_> = outcomes.try_iter().collect();
    if let Ok(result) = premature {
        results.push(result);
    }
    let expected = DirectFileSystem
        .open(&f.source.path)
        .unwrap()
        .read_exact_at(0, 100)
        .unwrap();
    assert_eq!(leader_result.unwrap(), expected);
    assert_eq!(results.len(), 2);
    for result in results {
        assert_eq!(
            result.expect("same-extent reader spent another scratch reservation"),
            expected
        );
    }
    assert_eq!(source.calls.load(Ordering::Acquire), 1);
    assert_eq!(f.source.bytes.load(Ordering::Acquire), EXTENT_BYTES);
    drop(image);
    assert_eq!(scratch.usage().unwrap(), ScratchUsage::default());
    f.idle();
}

#[test]
fn failed_or_panicked_receive_releases_its_charge_and_wakes_another_reader() {
    for first in [First::IoFailure, First::Corrupt, First::Panic] {
        let f = Fixture::new(8, 4096);
        let scratch = ScratchPool::new(ScratchLimits {
            max_bytes: EXTENT_BYTES,
            max_files: 1,
        })
        .unwrap();
        let gate = Gate::new(&f, scratch.clone(), first);
        let leader = {
            let image = gate.image.clone();
            thread::spawn(move || image.read_exact_at(0, 100))
        };
        gate.entered.recv_timeout(DEADLINE).unwrap();
        let (started, waiting) = mpsc::channel();
        let (finished, outcome) = mpsc::channel();
        let waiter = {
            let image = gate.image.clone();
            thread::spawn(move || {
                started.send(()).unwrap();
                finished.send(image.read_exact_at(0, 100)).unwrap();
            })
        };
        waiting.recv_timeout(DEADLINE).unwrap();
        let premature = outcome.recv_timeout(Duration::from_millis(200));
        gate.release.send(()).unwrap();
        let failed = leader.join();
        waiter.join().unwrap();
        assert!(
            matches!(premature, Err(mpsc::RecvTimeoutError::Timeout)),
            "reader did not wait for {first:?}"
        );
        match first {
            First::IoFailure => assert!(
                matches!(failed, Ok(Err(Error::Io(error))) if error.kind() == io::ErrorKind::ConnectionReset)
            ),
            First::Corrupt => assert!(matches!(failed, Ok(Err(Error::Integrity)))),
            First::Panic => assert!(failed.is_err()),
            First::Success => unreachable!(),
        }
        assert_eq!(
            outcome.recv_timeout(DEADLINE).unwrap().unwrap(),
            DirectFileSystem
                .open(&f.source.path)
                .unwrap()
                .read_exact_at(0, 100)
                .unwrap()
        );
        assert_eq!(gate.source.calls.load(Ordering::Acquire), 2);
        drop(gate);
        assert_eq!(scratch.usage().unwrap(), ScratchUsage::default());
        f.idle();
    }
}

#[test]
fn independent_extents_do_not_wait_for_an_unrelated_receive() {
    let f = Fixture::new(8, 4096);
    let scratch = ScratchPool::new(ScratchLimits {
        max_bytes: 2 * EXTENT_BYTES,
        max_files: 2,
    })
    .unwrap();
    let gate = Gate::new(&f, scratch.clone(), First::Success);
    let leader = {
        let image = gate.image.clone();
        thread::spawn(move || image.read_exact_at(0, 100))
    };
    gate.entered.recv_timeout(DEADLINE).unwrap();
    let (finished, outcome) = mpsc::channel();
    let other = {
        let image = gate.image.clone();
        thread::spawn(move || {
            finished
                .send(image.read_exact_at(EXTENT_BYTES, 100))
                .unwrap()
        })
    };
    let independent = outcome.recv_timeout(Duration::from_secs(2));
    gate.release.send(()).unwrap();
    leader.join().unwrap().unwrap();
    other.join().unwrap();
    assert_eq!(
        independent
            .expect("independent extent was serialized behind the held source")
            .unwrap(),
        DirectFileSystem
            .open(&f.source.path)
            .unwrap()
            .read_exact_at(EXTENT_BYTES, 100)
            .unwrap()
    );
    assert_eq!(gate.source.calls.load(Ordering::Acquire), 2);
    drop(gate);
    assert_eq!(scratch.usage().unwrap(), ScratchUsage::default());
    f.idle();
}
