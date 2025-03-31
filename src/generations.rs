use bincode::de::Decoder;
use bincode::enc::Encoder;
use bincode::error::{DecodeError, EncodeError};
use bincode::{Decode, Encode};
use rangemap::RangeMap;

use crate::{ColorFragmentIndex, ColorTableError, Result};

#[derive(Debug, PartialEq, Eq, Encode, Decode)]
enum GenerationState {
    None,                                // no generation has been started
    Ended(u64),                          // last generation number
    InProgress(u64, ColorFragmentIndex), // generation number, first fragment
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Generations {
    ranges: RangeMap<ColorFragmentIndex, u64>,
    state: GenerationState,
}

impl Encode for Generations {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        Encode::encode(&self.state, encoder)?;
        Encode::encode(
            &self
                .ranges
                .iter()
                .map(|(range, generation)| (range.start, range.end, *generation))
                .collect::<Vec<_>>(),
            encoder,
        )?;

        Ok(())
    }
}

impl<Context> Decode<Context> for Generations {
    fn decode<D: Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let state = Decode::decode(decoder)?;
        let gens_vec: Vec<(ColorFragmentIndex, ColorFragmentIndex, u64)> = Decode::decode(decoder)?;

        let mut generations = RangeMap::new();

        for (start, end, generation) in gens_vec.iter() {
            generations.insert(*start..*end, *generation);
        }

        generations
            .iter()
            .zip(gens_vec.iter())
            .all(|((range, generation), (start, end, generation_))| {
                &range.start == start && &range.end == end && generation == generation_
            })
            .then_some(())
            .ok_or(DecodeError::Other(
                "generations do not match (overlapping ranges?)",
            ))?;

        Ok(Self {
            ranges: generations,
            state,
        })
    }
}

impl Generations {
    pub fn new() -> Self {
        Self {
            ranges: RangeMap::new(),
            state: GenerationState::None,
        }
    }

    /// Get the end of the last generation
    fn last_range_end(&self) -> Option<&ColorFragmentIndex> {
        self.ranges.last_range_value().map(|(range, _)| &range.end)
    }

    /// Get the current in-progress generation
    pub fn current_generation(&self) -> Option<u64> {
        match self.state {
            GenerationState::InProgress(generation, _) => Some(generation),
            _ => None,
        }
    }

    /// Start a new generation at the given head fragment
    pub fn start_new_generation_at(
        &mut self,
        head: ColorFragmentIndex,
        generation: u64,
    ) -> Result<()> {
        match self.state {
            GenerationState::None => {
                // first generation must start at 0
                if !matches!(head, ColorFragmentIndex(0)) {
                    return Err(ColorTableError::InvalidGenerationState);
                }
                self.ranges.insert(head..head + 1, generation);
                self.state = GenerationState::InProgress(generation, head);
                Ok(())
            }
            GenerationState::Ended(last_generation) if last_generation < generation => {
                // don't overlap with previous generation
                if self.last_range_end().is_some_and(|last| last > &head) {
                    return Err(ColorTableError::InvalidGenerationState);
                }

                self.ranges.insert(head..head + 1, generation);

                // TODO: deferred writes

                self.state = GenerationState::InProgress(generation, head);
                Ok(())
            }
            _ => Err(ColorTableError::InvalidGeneration(generation)),
        }
    }

    /// End the current generation at the given head fragment
    pub fn end_current_generation_at(&mut self, head: ColorFragmentIndex) -> Result<()> {
        match self.state {
            GenerationState::InProgress(generation, old_head) if head > old_head => {
                debug_assert!(
                    self.ranges
                        .last_range_value()
                        .is_some_and(
                            |(range, _)| range.end == range.start + 1 && range.start == old_head
                        ),
                    "expected last generation to be a singleton starting at old head position ({:?}), got {:?}",
                    old_head,
                    self.ranges.last_range_value(),
                );

                self.ranges.insert(old_head..head, generation);
                self.state = GenerationState::Ended(generation);

                Ok(())
            }
            GenerationState::InProgress(generation, head) => {
                // generation would be empty, remove it
                self.ranges.remove(head..head + 1);
                self.state = GenerationState::Ended(generation);
                Ok(())
            }
            GenerationState::None | GenerationState::Ended(_) => {
                Err(ColorTableError::InvalidGenerationState)
            }
        }
    }

    /// Find the generation a fragment belongs to
    pub fn find(&self, idx: &ColorFragmentIndex) -> Option<&u64> {
        self.ranges.get(idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_deserialize() {
        let mut g = Generations::new();
        let mut head = ColorFragmentIndex(0);

        // start generation 1
        g.start_new_generation_at(head, 1).unwrap();

        // end generation 1
        head += 10;
        g.end_current_generation_at(head).unwrap();

        // start generation 2
        g.start_new_generation_at(head, 2).unwrap();

        // end generation 2
        head += 5;
        g.end_current_generation_at(head).unwrap();

        // start generation 4 (skip one)
        g.start_new_generation_at(head, 4).unwrap();

        // end generation 4
        head += 123456;
        g.end_current_generation_at(head).unwrap();

        let bytes = bincode::encode_to_vec(&g, crate::BINCODE_CONFIG).unwrap();
        dbg!(&bytes);
        let (deser, _) = bincode::decode_from_slice(&bytes, crate::BINCODE_CONFIG).unwrap();
        assert_eq!(g, deser);

        dbg!(&deser);
    }
}
