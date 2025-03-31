use bincode::{Decode, Encode};

use super::ColorFragmentIndex;

/// A generation is a range of color fragments that are all part of the same
/// epoch.
#[repr(C)]
#[derive(Clone, Copy, Encode, Decode)]
pub struct Generation {
    // range exclusive. could probably be inclusive, an empty generation doesn't
    // make sense (if it's empty, it's not a generation)
    start: ColorFragmentIndex,
    end: ColorFragmentIndex,
    generation: u64, // assuming this is strictly increasing. if not, need to fix
}

impl Generation {
    pub fn new(start: ColorFragmentIndex, generation: u64) -> Generation {
        Generation {
            start,
            end: start,
            generation,
        }
    }

    pub fn with_end(self, end: ColorFragmentIndex) -> Generation {
        Generation { end, ..self }
    }

    pub fn set_end(&mut self, end: ColorFragmentIndex) {
        self.end = end;
    }

    pub fn start(&self) -> ColorFragmentIndex {
        self.start
    }

    pub fn end(&self) -> ColorFragmentIndex {
        self.end
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}
