use std::ops::RangeInclusive;

use crate::ColorTableError;

use super::{ColorFragmentIndex, GenerationState};
use bincode::{Decode, Encode};
use rangemap::RangeInclusiveMap;

pub struct GenerationMap {
    generations: RangeInclusiveMap<ColorFragmentIndex, u64>,
    state: GenerationState,
}

impl GenerationMap {
    pub fn new() -> Self {
        GenerationMap {
            generations: RangeInclusiveMap::new(),
            state: GenerationState::Ended(0),
        }
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
}
