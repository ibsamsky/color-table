use color_table::{ColorFragment, ColorId, ColorTable, ColorTableConfig};

fn random_color(max_cardinality: u32) -> u64 {
    assert!(max_cardinality <= u64::BITS);
    let mut rng = fastrand::Rng::new();

    let mut color: u64 = 0;
    for _ in 0..max_cardinality {
        color ^= 1;
        color = color.rotate_left(rng.u32(..u64::BITS));
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
    let mut ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();
    dbg!(&ct);

    ct.start_generation(0).unwrap();
    ct.new_color_class(0x0123ABCD).unwrap();
    ct.end_generation().unwrap();

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
    let mut ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    ct.start_generation(0).unwrap();

    let now = std::time::Instant::now();
    for c in 0..N {
        ct.new_color_class(c as u64).unwrap();
    }
    display_timings!("insert new color", N, now.elapsed());

    ct.end_generation().unwrap();

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
    let mut ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    ct.start_generation(0).unwrap();
    let cc_id = ct.new_color_class(0x0123ABCD).unwrap();
    ct.end_generation().unwrap();

    ct.start_generation(1).unwrap();
    let fork_id = ct.fork_color_class(cc_id, 0xABCD0123).unwrap();
    ct.end_generation().unwrap();

    assert_eq!(cc_id, ColorId(1));
    assert_eq!(fork_id, ColorId(2));

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
    let mut ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    ct.start_generation(0).unwrap();
    let cc_id = ct.new_color_class(0x0123ABCD).unwrap();
    let unforked_id = ct.new_color_class(0x4567EF00).unwrap();
    ct.end_generation().unwrap();

    ct.start_generation(1).unwrap();
    let fork_id = ct.fork_color_class(cc_id, 0xABCD0123).unwrap();
    ct.end_generation().unwrap();

    ct.map().unwrap();

    assert_eq!(
        dbg!(ct.color_class(&fork_id)).collect::<Vec<_>>(),
        vec![(0xABCD0123, 1), (0x0123ABCD, 0)]
    );

    assert_eq!(
        ct.color_class(&unforked_id).collect::<Vec<_>>(),
        vec![(0x4567EF00, 0)]
    );

    ct.unmap();
}

// this test and `extend_many_and_iter` are slow, because they start and end generations an unreasonable number of times
// most of the time is spent doing 1,000,000 write syscalls
#[test]
fn fork_many_and_iter() {
    const N: usize = 1_000_000;

    let dir = tempfile::tempdir().unwrap();
    let mut ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    let now = std::time::Instant::now();
    ct.start_generation(0).unwrap();
    let mut cc_id = ct.new_color_class(0).unwrap();
    ct.end_generation().unwrap();
    for g in 1..N {
        ct.start_generation(g as u64).unwrap();
        cc_id = ct.fork_color_class(cc_id, g as u64).unwrap();
        ct.end_generation().unwrap();
    }
    display_timings!("fork color class", N, now.elapsed());

    ct.map().unwrap();

    let iter = ct.color_class(&cc_id);

    let now = std::time::Instant::now();
    for (i, (color, generation)) in (0..N).rev().zip(iter) {
        assert_eq!(color, i as u64);
        assert_eq!(generation, color);
    }
    let elapsed = now.elapsed();
    eprintln!(
        "iter {N} colors took {elapsed:?} ({:.2} ops/sec, {:?}/op)",
        N as f64 / elapsed.as_secs_f64(),
        elapsed / N as u32
    );
    ct.unmap();
}

#[test]
fn extend_one() {
    let dir = tempfile::tempdir().unwrap();
    let mut ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    ct.start_generation(0).unwrap();
    let cc_id = ct.new_color_class(0x0123ABCD).unwrap();
    ct.end_generation().unwrap();

    ct.start_generation(1).unwrap();
    ct.extend_color_class(cc_id, 0x4567EF00).unwrap();
    ct.end_generation().unwrap();

    ct.sync(None).unwrap();

    let table = std::fs::read(dir.path().join("color_table")).unwrap();
    // 2 + magic
    assert_eq!(table.len(), 3 * std::mem::size_of::<ColorFragment>());
    dbg!(bstr::BString::from(table));
    dbg!(&ct);

    ct.map().unwrap();
    assert_eq!(
        ct.color_class(&cc_id).collect::<Vec<_>>(),
        vec![(0x4567EF00, 1), (0x0123ABCD, 0)]
    );
    ct.unmap();
}

#[test]
fn extend_many_and_iter() {
    const N: usize = 1_000_000;

    let dir = tempfile::tempdir().unwrap();
    let mut ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    let now = std::time::Instant::now();
    ct.start_generation(0).unwrap();
    let cc_id = ct.new_color_class(0).unwrap();
    ct.end_generation().unwrap();
    for g in 1..N {
        ct.start_generation(g as u64).unwrap();
        ct.extend_color_class(cc_id, g as u64).unwrap();
        ct.end_generation().unwrap();
    }
    display_timings!("extend color class", N, now.elapsed());

    ct.map().unwrap();

    let iter = ct.color_class(&cc_id);

    let now = std::time::Instant::now();
    for (i, (color, generation)) in (0..N).rev().zip(iter) {
        assert_eq!(color, i as u64);
        assert_eq!(generation, color);
    }
    let elapsed = now.elapsed();
    eprintln!(
        "iter {N} colors took {elapsed:?} ({:.2} ops/sec, {:?}/op)",
        N as f64 / elapsed.as_secs_f64(),
        elapsed / N as u32
    );
    ct.unmap();
}

#[cfg(feature = "roaring")]
#[test]
fn intersect() {
    let dir = tempfile::tempdir().unwrap();
    let mut ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    ct.start_generation(0).unwrap();
    let cc1 = ct.new_color_class(0b1001000111010101111001101).unwrap();
    let cc2 = ct
        .new_color_class(0b1000101011001111110111100000000)
        .unwrap();
    ct.end_generation().unwrap();

    ct.start_generation(1).unwrap();
    ct.extend_color_class(cc1, 0b1000101011001111110111100000000)
        .unwrap();
    ct.extend_color_class(cc2, 0b1001000111010101111001101)
        .unwrap();
    ct.end_generation().unwrap();

    ct.sync(None).unwrap();

    // cc1 = 1011001111010101110001001000000...
    // cc2 = 0000000011110111111001101010001...
    //  &  = 0000000011010101110001001000000...

    let table = std::fs::read(dir.path().join("color_table")).unwrap();
    dbg!(bstr::BString::from(table));

    ct.map().unwrap();

    let now = std::time::Instant::now();
    let bm1 = ct.color_class(&cc1).into_bitmap();
    let bm2 = ct.color_class(&cc2).into_bitmap();
    let elapsed = now.elapsed();
    eprintln!("creating 2 bitmaps took {elapsed:?}");

    ct.unmap();

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
    let mut ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();

    ct.start_generation(0).unwrap();
    let cc1 = ct.new_color_class(get_color()).unwrap();
    let cc2 = ct.new_color_class(get_color()).unwrap();
    ct.end_generation().unwrap();

    // typical number of epochs for 100k samples
    const N: usize = 30;
    for g in 1..N {
        ct.start_generation(g as u64).unwrap();
        let c1 = get_color();
        let c2 = get_color();
        if c1 != 0 {
            ct.extend_color_class(cc1, c1).unwrap();
        }
        if c2 != 0 {
            ct.extend_color_class(cc2, c2).unwrap();
        }
        ct.end_generation().unwrap();
    }

    ct.sync(None).unwrap();

    let elapsed = now.elapsed();
    eprintln!("creating color table took {elapsed:?}");

    let table = std::fs::read(dir.path().join("color_table")).unwrap();
    dbg!(table.len());
    // dbg!(bstr::BString::from(table));

    ct.map().unwrap();

    let now = std::time::Instant::now();
    let bm1 = ct.color_class(&cc1).into_bitmap();
    let bm2 = ct.color_class(&cc2).into_bitmap();
    let elapsed = now.elapsed();
    eprintln!(
        "creating 2 bitmaps took {elapsed:?} ({:?}/sample)",
        elapsed / (N as u32 * u64::BITS) / 2
    );

    ct.unmap();

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

    let mut ct = ColorTable::load_or_new(&dir, config.clone()).unwrap();

    ct.start_generation(0).unwrap();
    let cc1 = ct.new_color_class(get_color()).unwrap();
    let cc2 = ct.new_color_class(get_color()).unwrap();
    ct.end_generation().unwrap();

    ct.start_generation(1).unwrap();
    ct.extend_color_class(cc1, get_color()).unwrap();
    let cc3 = ct.fork_color_class(cc2, get_color()).unwrap();
    ct.end_generation().unwrap();

    ct.sync(None).unwrap();
    let mut ct2 = ColorTable::load(&dir, config).unwrap();

    // concurrent read access is supported
    ct.map().unwrap();
    ct2.map().unwrap();

    for cc in [&cc1, &cc2, &cc3] {
        #[cfg(feature = "roaring")]
        assert_eq!(
            ct.color_class(cc).into_bitmap(),
            ct2.color_class(cc).into_bitmap()
        );
        assert_eq!(
            ct.color_class(cc).collect::<Vec<_>>(),
            ct2.color_class(cc).collect::<Vec<_>>()
        )
    }
}
