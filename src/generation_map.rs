use std::{fs, ops::RangeInclusive};

use crate::ColorTableError;

use crate::color_table::ColorFragmentIndex;
use bincode::{
    Decode, Encode,
    de::Decoder,
    enc::Encoder,
    error::{DecodeError, EncodeError},
};
use rangemap::RangeInclusiveMap;

const OUT_FILE_NAME: &str = "generation_map";

#[derive(bincode::Encode, bincode::Decode, PartialEq, Debug)]
enum GenerationState {
    Ended(u64),             // last generation number
    InProgress(u64, usize), // generation number, number of fragments at start of generation
}

#[derive(PartialEq, Debug)]
pub struct GenerationMap {
    generations: RangeInclusiveMap<ColorFragmentIndex, u64>,
    state: GenerationState,
}

impl Encode for GenerationMap {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        Encode::encode(&self.state, encoder)?;
        Encode::encode(
            &self
                .generations
                .iter()
                .map(|(range, generation)| (*range.start(), *range.end(), *generation))
                .collect::<Vec<_>>(),
            encoder,
        )?;

        Ok(())
    }
}

impl<Context> Decode<Context> for GenerationMap {
    fn decode<D: Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let state: GenerationState = Decode::decode(decoder)?;
        let gens_vec: Vec<(ColorFragmentIndex, ColorFragmentIndex, u64)> = Decode::decode(decoder)?;

        let mut generations = RangeInclusiveMap::new();

        for (start, end, generation) in gens_vec {
            println!(
                "start: {:?}, end: {:?}, generation: {:?}",
                start, end, generation
            );
            generations.insert(start..=end, generation);
        }
        Ok(Self {
            state: state,
            generations: generations,
        })
    }
}

impl GenerationMap {
    pub fn new() -> Self {
        GenerationMap {
            generations: RangeInclusiveMap::new(),
            state: GenerationState::Ended(0),
        }
    }
    pub fn from_serialized(&self) -> Self {
        let mut file = fs::File::open(OUT_FILE_NAME).expect("failed to create file");
        bincode::decode_from_std_read(&mut file, crate::BINCODE_CONFIG)
            .expect("deserialization failed")
    }

    pub fn serialize(&self) {
        let mut out = fs::File::create(OUT_FILE_NAME).expect("failed to create file");
        bincode::encode_into_std_write(self, &mut out, crate::BINCODE_CONFIG)
            .expect("serialization failed");
    }

    pub fn last_generation(&self) -> Option<&RangeInclusive<ColorFragmentIndex>> {
        // Option<(&RangeInclusive<ColorFragmentIndex>, &u64)>
        match self.generations.last_range_value() {
            Some((range, _generation)) => Some(range),
            None => None,
        }
    }

    pub fn set_last_generation_end(&mut self, end: ColorFragmentIndex) {
        if let Some((range, generation)) = self.generations.last_range_value() {
            self.generations
                .insert(*range.end()..=end, generation.clone());
        }
    }

    pub fn start_generation(
        &mut self,
        start: ColorFragmentIndex,
        generation: u64,
        fragments: usize,
    ) -> Result<(), ColorTableError> {
        match self.state {
            GenerationState::Ended(last_generation) if generation > last_generation => {
                self.state = GenerationState::InProgress(generation, fragments);
                self.generations.insert(start..=start, generation);

                // TODO: deferred writes
                Ok(())
            }
            _ => Err(ColorTableError::InvalidGeneration(generation)),
        }
    }

    pub fn end_generation(&mut self, cur_fragments: usize) -> Result<(), ColorTableError> {
        match self.state {
            GenerationState::InProgress(generation, fragments) if cur_fragments > fragments => {
                let Some(_last_generation) = self.last_generation() else {
                    unreachable!() // we just checked that a generation is in progress. if we get here, something is VERY wrong
                };
                self.set_last_generation_end(ColorFragmentIndex(
                    cur_fragments.try_into().expect("too many fragments"),
                ));
                self.state = GenerationState::Ended(generation);
                Ok(())
            }
            GenerationState::InProgress(generation, _) | GenerationState::Ended(generation) => {
                Err(ColorTableError::InvalidGeneration(generation))
            }
        }
    }

    pub fn find(&self, idx: ColorFragmentIndex) -> Option<&u64> {
        self.generations.get(&idx)
    }
}
