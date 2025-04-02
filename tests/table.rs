use color_table::{ColorFragment, ColorId, ColorTable, ColorTableConfig};

#[test]
fn new_one() {
    let dir = tempfile::tempdir().unwrap();
    let mut ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();
    dbg!(&ct);

    ct.start_generation(0).unwrap();
    ct.new_color_class(0x0123ABCD).unwrap();
    ct.end_generation().unwrap();

    ct.sync().unwrap();

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
    let elapsed = now.elapsed();
    eprintln!(
        "insert {N} colors took {elapsed:?} ({:.2} ops/sec, {:?}/op)",
        N as f64 / elapsed.as_secs_f64(),
        elapsed / N as u32
    );

    ct.end_generation().unwrap();

    ct.sync().unwrap();

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

    ct.sync().unwrap();

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
    let elapsed = now.elapsed();
    eprintln!(
        "fork {N} colors took {elapsed:?} ({:.2} ops/sec, {:?}/op)",
        N as f64 / elapsed.as_secs_f64(),
        elapsed / N as u32
    );

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

    ct.sync().unwrap();

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
    let elapsed = now.elapsed();
    eprintln!(
        "extend {N} colors took {elapsed:?} ({:.2} ops/sec, {:?}/op)",
        N as f64 / elapsed.as_secs_f64(),
        elapsed / N as u32
    );

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
