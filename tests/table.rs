use color_table::{ColorFragment, ColorId, ColorTable, ColorTableConfig};

#[test]
fn new_one() {
    let dir = tempfile::tempdir().unwrap();
    let mut ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();
    dbg!(&ct);

    ct.start_generation(0).unwrap();
    ct.new_color_class(0x0123ABCD).unwrap();
    ct.end_generation().unwrap();

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
        "insert {N} color classes took {elapsed:?} ({:.2} ops/sec, {:?}/op)",
        N as f64 / elapsed.as_secs_f64(),
        elapsed / N as u32
    );

    ct.end_generation().unwrap();

    let ct_file = std::fs::read(dir.path().join("color_table")).unwrap();
    assert_eq!(
        ct_file.len(),
        std::mem::size_of::<ColorFragment>() * (N + 1)
    );

    // dbg!(ct_file);
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

    let table = std::fs::read(dir.path().join("color_table")).unwrap();
    assert_eq!(table.len(), 3 * std::mem::size_of::<ColorFragment>());
    dbg!(&table);
    dbg!(ct);
}
