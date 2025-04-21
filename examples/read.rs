use color_table::{ColorFragment, ColorId, ColorTable, ColorTableConfig};

fn main() {
    let n = std::env::args()
        .nth(1)
        .map_or(1000, |s| s.parse().expect("failed to parse number"));

    let dir = tempfile::tempdir().unwrap();

    let mut ct = ColorTable::new(&dir, ColorTableConfig::default()).unwrap();
    let now = std::time::Instant::now();
    ct.start_generation(0).unwrap();
    let (colors, color_ids): (Vec<u64>, Vec<ColorId>) = (0..n)
        .map(|i| (i as u64, ct.new_color_class(i as u64).unwrap()))
        .unzip();

    ct.end_generation().unwrap();

    let elapsed = now.elapsed();
    eprintln!(
        "inserted {n} colors in {elapsed:?} ({:?}/color, {:.2} colors/sec)",
        elapsed / n as u32,
        n as f64 / elapsed.as_secs_f64()
    );

    let file = std::fs::read(dir.path().join("color_table")).unwrap();
    assert_eq!(file.len(), (n + 1) * std::mem::size_of::<ColorFragment>());

    // simulate typical read workload (essentially random access)
    let mut reads = color_ids.iter().enumerate().collect::<Vec<_>>();
    const NUM_READS: usize = 3;

    let now = std::time::Instant::now();
    ct.map().unwrap();
    for _ in 0..NUM_READS {
        fastrand::shuffle(&mut reads);
        for (i, color_id) in reads.iter() {
            let color = ct.color_class(color_id).next().unwrap().0;
            assert_eq!(color, colors[*i]);
        }
    }
    ct.unmap();
    let elapsed = now.elapsed();
    eprintln!(
        "read {} colors in {elapsed:?} ({:?}/color, {:.2} colors/sec)",
        n * NUM_READS,
        elapsed / (n * NUM_READS) as u32,
        (n * NUM_READS) as f64 / elapsed.as_secs_f64()
    );

    ct.sync(None).unwrap();
}
