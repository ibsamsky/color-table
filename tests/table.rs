use color_table::{ColorFragment, ColorId, ColorTable, ColorTableConfig};

fn random_color(max_cardinality: u32) -> u32 {
    assert!(max_cardinality <= u32::BITS);
    let mut rng = fastrand::Rng::new();

    let mut color: u32 = 0;
    for _ in 0..max_cardinality {
        color ^= 1;
        color = color.rotate_left(rng.u32(..u32::BITS));
    }
    color
}

macro_rules! display_timings {
    ($what:literal, $n:expr, $elapsed:expr) => {{
        let elapsed: ::std::time::Duration = $elapsed;
        let n = $n;
        eprintln!(
            concat!($what, " (x{}) took {:?} ({:.2} ops/sec, {:?}/op)"),
            n,
            elapsed,
            n as f64 / elapsed.as_secs_f64(),
            elapsed / n as u32
        );
    }};
    ($n:expr, $elapsed:expr) => {
        display_timings!("operation", $n, $elapsed);
    };
}

#[test]
fn new_one() {
    let dir = tempfile::tempdir().unwrap();
    let ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();
    dbg!(&ct);

    ct.with_generation(0, |ct| {
        ct.new_color_class(0x0123ABCD).unwrap();
    })
    .unwrap();

    ct.sync(None).unwrap();

    let table = std::fs::read(dir.path().join("color_table")).unwrap();
    assert_eq!(table.len(), 2 * std::mem::size_of::<ColorFragment>());
    dbg!(&table);
    dbg!(ct);
}

#[test]
fn new_many() {
    const N: usize = 1_000_000;

    let dir = tempfile::tempdir().unwrap();
    let ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    ct.with_generation(0, |ct| {
        let now = std::time::Instant::now();
        for c in 0..N {
            ct.new_color_class(c as u32).unwrap();
        }
        display_timings!("insert new color", N, now.elapsed());
    })
    .unwrap();

    ct.sync(None).unwrap();

    let ct_file = std::fs::read(dir.path().join("color_table")).unwrap();
    // N + magic
    assert_eq!(
        dbg!(ct_file.len()),
        std::mem::size_of::<ColorFragment>() * (N + 1)
    );

    // dbg!(bstr::BString::from(ct_file)); // too big :)
    // dbg!(ct);
}

#[test]
fn fork_one() {
    let dir = tempfile::tempdir().unwrap();
    let ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    let cc_id = ct
        .with_generation(0, |ct| ct.new_color_class(0x0123ABCD).unwrap())
        .unwrap();

    let fork_id = ct
        .with_generation(1, |ct| ct.fork_color_class(cc_id, 0xABCD0123).unwrap())
        .unwrap();

    assert_eq!(cc_id, ColorId::new(1));
    assert_eq!(fork_id, ColorId::new(2));

    ct.sync(None).unwrap();

    let table = std::fs::read(dir.path().join("color_table")).unwrap();
    // 2 + magic
    assert_eq!(table.len(), 3 * std::mem::size_of::<ColorFragment>());
    dbg!(bstr::BString::from(table));
    dbg!(ct);
}

#[test]
fn fork_one_and_iter() {
    let dir = tempfile::tempdir().unwrap();
    let ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    let (cc_id, unforked_id) = ct
        .with_generation(0, |ct| {
            (
                ct.new_color_class(0x0123ABCD).unwrap(),
                ct.new_color_class(0x4567EF00).unwrap(),
            )
        })
        .unwrap();

    let fork_id = ct
        .with_generation(1, |ct| ct.fork_color_class(cc_id, 0xABCD0123).unwrap())
        .unwrap();

    let ct_map = ct.map().unwrap();

    assert_eq!(
        dbg!(ct_map.color_class(&fork_id)).collect::<Vec<_>>(),
        vec![(0xABCD0123, 1), (0x0123ABCD, 0)]
    );

    assert_eq!(
        ct_map.color_class(&unforked_id).collect::<Vec<_>>(),
        vec![(0x4567EF00, 0)]
    );
}

// this test and `extend_many_and_iter` are slow, because they start and end generations an unreasonable number of times
// most of the time is spent doing 1,000,000 write syscalls
#[test]
fn fork_many_and_iter() {
    const N: usize = 1_000_000;

    let dir = tempfile::tempdir().unwrap();
    let ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    let now = std::time::Instant::now();
    let mut cc_id = ct
        .with_generation(0, |ct| ct.new_color_class(0).unwrap())
        .unwrap();
    for g in 1..N {
        ct.with_generation(g as u64, |ct| {
            cc_id = ct.fork_color_class(cc_id, g as u32).unwrap();
        })
        .unwrap();
    }

    display_timings!("fork color class", N, now.elapsed());

    let ct_map = ct.map().unwrap();

    let iter = ct_map.color_class(&cc_id);

    let now = std::time::Instant::now();
    for (i, (color, generation)) in (0..N).rev().zip(iter) {
        assert_eq!(color, i as u32);
        assert_eq!(generation, color.into());
    }
    let elapsed = now.elapsed();
    eprintln!(
        "iter {N} colors took {elapsed:?} ({:.2} ops/sec, {:?}/op)",
        N as f64 / elapsed.as_secs_f64(),
        elapsed / N as u32
    );
}

#[test]
fn extend_one() {
    let dir = tempfile::tempdir().unwrap();
    let ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    let mut cc_id = ct
        .with_generation(0, |ct| ct.new_color_class(0x0123ABCD).unwrap())
        .unwrap();

    ct.with_generation(1, |ct| {
        cc_id = ct.extend_color_class(cc_id, 0x4567EF00).unwrap();
    })
    .unwrap();

    ct.sync(None).unwrap();

    let table = std::fs::read(dir.path().join("color_table")).unwrap();
    // 2 + magic
    assert_eq!(table.len(), 3 * std::mem::size_of::<ColorFragment>());
    dbg!(bstr::BString::from(table));
    dbg!(&ct);

    let ct_map = ct.map().unwrap();
    assert_eq!(
        ct_map.color_class(&cc_id).collect::<Vec<_>>(),
        vec![(0x4567EF00, 1), (0x0123ABCD, 0)]
    );
}

#[test]
fn extend_many_and_iter() {
    const N: usize = 1_000_000;

    let dir = tempfile::tempdir().unwrap();
    let ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    let now = std::time::Instant::now();
    let mut cc_id = ct
        .with_generation(0, |ct| ct.new_color_class(0).unwrap())
        .unwrap();
    for g in 1..N {
        ct.with_generation(g as u64, |ct| {
            cc_id = ct.extend_color_class(cc_id, g as u32).unwrap()
        })
        .unwrap();
    }
    display_timings!("extend color class", N, now.elapsed());

    let ct_map = ct.map().unwrap();
    let iter = ct_map.color_class(&cc_id);

    let now = std::time::Instant::now();
    for (i, (color, generation)) in (0..N).rev().zip(iter) {
        assert_eq!(color, i as u32);
        assert_eq!(generation, color.into());
    }
    let elapsed = now.elapsed();
    eprintln!(
        "iter {N} colors took {elapsed:?} ({:.2} ops/sec, {:?}/op)",
        N as f64 / elapsed.as_secs_f64(),
        elapsed / N as u32
    );
}

#[test]
fn small_threaded() {
    const TOTAL_POW: u32 = 25;

    const THREADS_POW: u32 = 3;
    const PER_THREAD_POW: u32 = TOTAL_POW - THREADS_POW;

    const THREADS: usize = 1 << THREADS_POW;
    const PER_THREAD: usize = 1 << PER_THREAD_POW;

    const TOTAL: usize = (THREADS - 1) << PER_THREAD_POW | (PER_THREAD - 1); // usize::MAX

    let dir = tempfile::tempdir().unwrap();
    let config = ColorTableConfig::default();

    let ct = ColorTable::new(&dir, config).unwrap();

    let now = std::time::Instant::now();
    let cids = ct
        .with_generation(0, |ref ct| {
            std::thread::scope(|s| {
                let mut handles = Vec::new();

                for t in 0..THREADS {
                    let handle = s.spawn(move || {
                        let mut cids = Vec::new();
                        for j in 0..PER_THREAD {
                            let color = (t << PER_THREAD_POW) | j;
                            let cid = ct.new_color_class(color as u32).unwrap();
                            cids.push(cid);
                        }
                        cids
                    });
                    handles.push(handle);
                }

                let mut cids = Vec::new();
                for handle in handles {
                    let thread_cids = handle.join().unwrap();
                    cids.push(thread_cids);
                }
                cids
            })
        })
        .unwrap();

    let elapsed = now.elapsed();
    display_timings!("insert color classes", TOTAL, elapsed);

    let now = std::time::Instant::now();
    let map = ct.map().unwrap();
    let mut colors = std::thread::scope(|s| {
        // scoped because of borrowed ct

        let mut handles = Vec::new();
        for thread_cids in cids {
            let handle = s.spawn(|| {
                let mut colors = Vec::new();
                for cid in thread_cids {
                    let class = map.color_class(&cid).map(|(c, _)| c);
                    colors.extend(class);
                }
                colors
            });
            handles.push(handle);
        }

        let mut colors = Vec::new();
        for handle in handles {
            let thread_colors = handle.join().unwrap();
            colors.extend(thread_colors);
        }
        colors
    });
    let elapsed = now.elapsed();
    display_timings!("retrieve color classes", TOTAL, elapsed);

    colors.sort_unstable();

    assert!((0..TOTAL as u32).all(|c| colors.binary_search(&c).is_ok()));
}

#[test]
fn concurrent_query() {
    let dir = tempfile::tempdir().unwrap();
    let ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    let timer = std::time::Instant::now();

    std::thread::scope(|s| {
        let rct = &ct;
        let jh1 = s.spawn(move || {
            rct.with_generation(0, |ct| {
                ct.new_color_class(0b1001000111010101111001101).unwrap();
                std::thread::sleep(std::time::Duration::from_millis(100)); // hold whatever lock for a bit
            })
            .unwrap()
        });

        let jh2 = s.spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            // this query should not be blocked by the above generation in progress
            let color = rct
                .map()
                .unwrap()
                .color_class(&ColorId::new(1))
                .collect::<Vec<_>>();
            std::thread::sleep(std::time::Duration::from_millis(100));
            color
        });

        let color = jh2.join().unwrap();
        assert_eq!(color, vec![(0b1001000111010101111001101, 0)]);
        jh1.join().unwrap();
    });

    let elapsed = timer.elapsed();
    eprintln!("concurrent query took {elapsed:?}");
    if elapsed.as_millis() > 120 {
        panic!("concurrent query took too long");
    }
}

#[cfg(feature = "roaring")]
#[test]
fn intersect() {
    let dir = tempfile::tempdir().unwrap();
    let ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    let (cc1, cc2) = ct
        .with_generation(0, |ct| {
            (
                ct.new_color_class(0b1001000111010101111001101).unwrap(),
                ct.new_color_class(0b1000101011001111110111100000000)
                    .unwrap(),
            )
        })
        .unwrap();

    ct.with_generation(1, |ct| {
        ct.extend_color_class(cc1, 0b1000101011001111110111100000000)
            .unwrap();
        ct.extend_color_class(cc2, 0b1001000111010101111001101)
            .unwrap();
    })
    .unwrap();

    ct.sync(None).unwrap();

    // cc1 = 1011001111010101110001001000000...
    // cc2 = 0000000011110111111001101010001...
    //  &  = 0000000011010101110001001000000...

    let table = std::fs::read(dir.path().join("color_table")).unwrap();
    dbg!(bstr::BString::from(table));

    let ct_map = ct.map().unwrap();

    let now = std::time::Instant::now();
    let bm1 = ct_map.color_class(&cc1).into_bitmap();
    let bm2 = ct_map.color_class(&cc2).into_bitmap();
    let elapsed = now.elapsed();
    eprintln!("creating 2 bitmaps took {elapsed:?}");

    let now = std::time::Instant::now();
    let intersection = bm1 & bm2;
    let elapsed = now.elapsed();
    eprintln!("intersection took {elapsed:?}");

    dbg!(&intersection);

    assert_eq!(intersection.range_cardinality(0..=7), 0);
    assert!(intersection.contains_range(8..=9));
    assert!(!intersection.contains(10));
    assert!(intersection.contains(11));
    // ... etc.
}

#[cfg(feature = "roaring")]
#[test]
fn large_extend_intersect() {
    let get_color = || random_color(32);

    let dir = tempfile::tempdir().unwrap();

    let now = std::time::Instant::now();
    let ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    let (cc1, cc2) = ct
        .with_generation(0, |ct| {
            (
                ct.new_color_class(get_color()).unwrap(),
                ct.new_color_class(get_color()).unwrap(),
            )
        })
        .unwrap();

    // typical number of epochs for 100k samples
    const N: usize = 30;
    for g in 1..N {
        ct.with_generation(g as u64, |ct| {
            let c1 = get_color();
            let c2 = get_color();
            if c1 != 0 {
                ct.extend_color_class(cc1, c1).unwrap();
            }
            if c2 != 0 {
                ct.extend_color_class(cc2, c2).unwrap();
            }
        })
        .unwrap();
    }

    ct.sync(None).unwrap();

    let elapsed = now.elapsed();
    eprintln!("creating color table took {elapsed:?}");

    let table = std::fs::read(dir.path().join("color_table")).unwrap();
    dbg!(table.len());

    let ct_map = ct.map().unwrap();

    let now = std::time::Instant::now();
    let bm1 = ct_map.color_class(&cc1).into_bitmap();
    let bm2 = ct_map.color_class(&cc2).into_bitmap();
    let elapsed = now.elapsed();
    eprintln!(
        "creating 2 bitmaps took {elapsed:?} ({:?}/sample)",
        elapsed / (N as u32 * u64::BITS) / 2
    );

    let now = std::time::Instant::now();
    let intersection = bm1 & bm2;
    let elapsed = now.elapsed();
    eprintln!("intersection took {elapsed:?}");

    dbg!(&intersection);
    dbg!(intersection.serialized_size());
}

#[test]
fn sync_and_load() {
    let get_color = || random_color(16);
    let dir = tempfile::tempdir().unwrap();
    let config = ColorTableConfig::default();

    let ct = ColorTable::load_or_new(&dir, config.clone()).unwrap();

    let (cc1, cc2) = ct
        .with_generation(0, |ct| {
            (
                ct.new_color_class(get_color()).unwrap(),
                ct.new_color_class(get_color()).unwrap(),
            )
        })
        .unwrap();

    let cc3 = ct
        .with_generation(1, |ct| {
            ct.extend_color_class(cc1, get_color()).unwrap();
            ct.fork_color_class(cc2, get_color()).unwrap()
        })
        .unwrap();

    ct.sync(None).unwrap();
    let ct2 = ColorTable::load(&dir, config).unwrap();

    // concurrent read access is supported
    let ct_map = ct.map().unwrap();
    let ct2_map = ct2.map().unwrap();

    for cc in [&cc1, &cc2, &cc3] {
        #[cfg(feature = "roaring")]
        assert_eq!(
            ct_map.color_class(cc).into_bitmap(),
            ct2_map.color_class(cc).into_bitmap()
        );
        assert_eq!(
            ct_map.color_class(cc).collect::<Vec<_>>(),
            ct2_map.color_class(cc).collect::<Vec<_>>()
        )
    }
}

#[test]
fn load_and_write() {
    let get_color = || random_color(16);
    let dir = tempfile::tempdir().unwrap();
    let config = ColorTableConfig::default();

    let ct = ColorTable::load_or_new(&dir, config.clone()).unwrap();

    ct.with_generation(0, |ct| {
        ct.new_color_class(get_color()).unwrap();
    })
    .unwrap();

    ct.sync(None).unwrap();

    let ct = ColorTable::load(&dir, config).unwrap();
    assert!(ct.with_generation(0, |_| {}).is_err());

    ct.with_generation(1, |ct| {
        ct.new_color_class(get_color()).unwrap();
    })
    .unwrap();
    ct.sync(None).unwrap();
}
