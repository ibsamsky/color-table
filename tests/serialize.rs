use color_table::{color_table::ColorFragmentIndex, generation_map::GenerationMap};

#[test]
fn test_generation_map_serialization() {
    let mut generation_map = GenerationMap::new();
    let mut fragments: usize = 0;

    // Start generation 1
    generation_map
        .start_generation(
            ColorFragmentIndex(fragments.try_into().expect("too many fragments")),
            1, // generation number
            fragments,
        )
        .unwrap();

    // Extend generation 1
    fragments += 10;
    generation_map.set_last_generation_end(ColorFragmentIndex(
        fragments.try_into().expect("too many fragments"),
    ));
    // End generation 1
    fragments += 3;
    generation_map.end_generation(fragments).unwrap();

    // Start generation 2
    generation_map
        .start_generation(
            ColorFragmentIndex(fragments.try_into().expect("too many fragments")),
            2, // generation number
            fragments,
        )
        .unwrap();
    // Extend generation 2
    fragments += 5;
    generation_map.set_last_generation_end(ColorFragmentIndex(
        fragments.try_into().expect("too many fragments"),
    ));
    // End generation 2
    fragments += 2;
    generation_map.end_generation(fragments).unwrap();

    generation_map.serialize();
    let deserialized_map = generation_map.from_serialized();
    assert_eq!(generation_map, deserialized_map);
    println!("Deserialized map: {:?}", deserialized_map);
}
