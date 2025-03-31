//! color table!
//!
//! ### Terms:
//!
//! - color class: a set of color fragments. essentially one "color". analogous to a large bitvec/bitset
//!   - the null color class: a color class with id `0`, with no fragments
//!   - tail fragment: the last fragment of a color class
//! - color fragment: a "partial color" (bitset with 64 items) and a pointer to the parent fragment
//! - generation: a range of color fragments that are all part of the same epoch
//!
//! ### Definitions:
//!
//! - let `C` be the set of color classes (including the null color class)
//! - let `F` be the set of color fragments.
//! - let `G` be the set of generations
//!
//! ### Facts:
//!
//! - each color class in `C` is a subset of `F`
//! - `C` is a [cover](https://en.wikipedia.org/wiki/Cover_(topology)) of `F`
//! - each generation in `G` is a subset of `F`
//! - the fragments in `F` are linearly ordered by their index in the table
//!   - `G` is a [noncrossing partition](https://en.wikipedia.org/wiki/Noncrossing_partition) of `F` with respect to this order
//! - the current generation contains at most one color fragment for each color class
//!   - each color fragment in the current generation belongs to a unique color class
//!   - each existing color class may be extended at most once during a generation
//!   - an existing color class can be forked any number of times during a generation (as this creates a new color class)
//!   - any new color class resulting from a fork or extend cannot be forked or extended within the same generation. this would cause conflicting colors to exist in the same generation
//! - color classes can be forked. the resulting new color class will share all existing fragments with the parent color class, but any new fragments will only belong to the forked color class
//!   - in other words, all fragments up to the point of the fork are shared by both the parent and the forked color class
//!   - the parent color class will continue to exist and can be forked or extended as normal
//! - color classes can be extended. this simply adds a new fragment to the color class
//!   - updating the tail fragment of the color class is deferred until the next generation. this allows the index to be forked from the old tail fragment until the next generation
//!
//! in our use case (kmer to sample mapping), the following is also true:
//! - for the set of kmers `K`, the mapping `K -> C` is surjective.
//!   - that is, each color class may correspond to multiple kmers, and every possible kmer maps to some color class (for the vast majority of kmers, this is the null color class)
//! - in order to save space, fragments are (read: should be) stored iff they contain at least one set bit

mod generation;
mod generation_map;

use std::fs::File;
use std::io::BufWriter;
use std::ops::{Deref, RangeInclusive};
use std::path::{Path, PathBuf};

use bincode::{Decode, Encode};
use bytemuck::{Pod, PodCastError, Zeroable};
use generation::Generation;
use generation_map::GenerationMap;
use rangemap::StepLite;

use crate::{ColorTableError, Result};

#[derive(Clone, Copy, Debug, Zeroable, Pod, Encode, Decode, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
struct ColorFragmentIndex(u32);
impl StepLite for ColorFragmentIndex {
    fn add_one(&self) -> Self {
        ColorFragmentIndex(self.0 + 1)
    }
    fn sub_one(&self) -> Self {
        ColorFragmentIndex(self.0 - 1)
    }
}

// make ColorFragmentIndex act like a u32
impl Deref for ColorFragmentIndex {
    type Target = u32;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Zeroable, Pod, Encode, Decode)]
#[repr(transparent)]
struct ColorId(u32);

// ditto above
impl Deref for ColorId {
    type Target = u32;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
struct ColorFragment {
    color: u64,
    parent_pointer: ColorFragmentIndex,
}

impl ColorFragment {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }

    #[inline]
    fn try_from_bytes(bytes: &[u8]) -> Result<&Self, PodCastError> {
        bytemuck::try_from_bytes(bytes)
    }

    // this is generally unnecessary because of mmap fuckery
    #[inline]
    fn from_bytes(bytes: &[u8]) -> &Self {
        // due to repr(packed), infallible except for size mismatch
        bytemuck::from_bytes(bytes)
    }
}

/// Deferred update to a color class' tail fragment.
#[derive(Debug)]
struct DeferredUpdate {
    color_id: ColorId,
    parent: ColorFragmentIndex,
}

/// placeholder
struct ColorTableMmap;

enum GenerationState {
    Ended(u64),             // last generation number
    InProgress(u64, usize), // generation number, number of fragments at start of generation
}

pub struct ColorTable {
    directory: PathBuf,
    file: BufWriter<File>,
    mmap: Option<ColorTableMmap>,
    // mysterious field that is never read
    needs_remap: bool,
    fragments: usize, // should probably be a u32 (to match ColorFragmentIndex)
    color_id_to_last_fragment_mapping: Vec<ColorFragmentIndex>,
    delayed_writes: Vec<DeferredUpdate>,
    generations: GenerationMap, // generation_ranges: Vec<Generation>, // RangeMap<ColorFragmentIndex, u64>
                                // generation_state: GenerationState,
}

impl ColorTable {
    pub fn load_or_init(dir: impl AsRef<Path>) -> Result<Self> {
        todo!()
    }

    fn last_generation(&self) -> Option<&RangeInclusive<ColorFragmentIndex>> {
        self.generations.last_generation()
        // self.generation_ranges.last()
    }

    // fn last_generation_mut(&mut self) -> Option<&mut RangeInclusive<ColorFragmentIndex>> {
    //     // self.generation_ranges.last_mut()
    // }

    pub fn start_generation(&mut self, generation: u64) -> Result<()> {
        self.generations.start_generation(
            ColorFragmentIndex(self.fragments.try_into().expect("too many fragments")),
            generation,
            self.fragments,
        )
    }

    pub fn end_generation(&mut self) -> Result<()> {
        self.generations.end_generation(self.fragments)
    }
}
