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
//! - fragment indexes start at 1. fragment 0 is reserved as the parent of the head fragment in a color class
//!   - each fragment can be found at offset `sizeof::<ColorFragment>() * fragment_index` in the color table file
//!
//! in our use case (kmer to sample mapping), the following is also true:
//! - for the set of kmers `K`, the mapping `K -> C` is surjective.
//!   - that is, each color class may correspond to multiple kmers, and every possible kmer maps to some color class (for the vast majority of kmers, this is the null color class)
//! - in order to save space, fragments are (read: should be) stored iff they contain at least one set bit

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use bincode::{Decode, Encode};
use bytemuck::{Pod, PodCastError, Zeroable};

use crate::generations::Generations;
use crate::{ColorTableConfig, ColorTableError, Result};

#[derive(Clone, Copy, Debug, Zeroable, Pod, Encode, Decode, Ord, PartialOrd, Eq, PartialEq)]
#[repr(transparent)]
pub struct ColorFragmentIndex(pub u32); // up to 4b fragments/colors

impl std::ops::Add<u32> for ColorFragmentIndex {
    type Output = Self;

    #[inline]
    fn add(self, other: u32) -> Self {
        let res = if cfg!(debug_assertions) {
            self.0.checked_add(other).expect("overflow")
        } else {
            self.0.wrapping_add(other)
        };

        Self(res)
    }
}

impl std::ops::AddAssign<u32> for ColorFragmentIndex {
    #[inline]
    fn add_assign(&mut self, other: u32) {
        let res = if cfg!(debug_assertions) {
            self.0.checked_add(other).expect("overflow")
        } else {
            self.0.wrapping_add(other)
        };

        self.0 = res;
    }
}

#[derive(Clone, Copy, Debug, Zeroable, Pod, Encode, Decode)]
#[repr(transparent)]
pub struct ColorId(u32);

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct ColorFragment {
    color: u64,
    parent_pointer: ColorFragmentIndex, // or Option<NonZero<ColorFragmentIndex>> or something like that
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
#[derive(Debug)]
struct ColorTableMmap;

#[derive(Debug)]
pub struct ColorTable {
    directory: PathBuf,
    file: BufWriter<File>,
    mmap: Option<ColorTableMmap>,
    // mysterious field that is never read
    needs_remap: bool,
    head: ColorFragmentIndex, // more or less file offset of last fragment
    color_id_to_last_fragment_mapping: Vec<ColorFragmentIndex>,
    delayed_writes: Vec<DeferredUpdate>,
    generations: Generations,
}

impl ColorTable {
    pub fn new(dir: impl AsRef<Path>, config: ColorTableConfig) -> Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(&dir.as_ref().join(config.color_table_file_name))?;

        let mut file = BufWriter::with_capacity(config.buffer_size, file);
        // 12 bytes magic header to make offset calculations easier - maybe store len/format version/checksum later
        file.write_all(b"CTBL\0\0\0\0\0\0\0\0").unwrap();

        Ok(Self {
            directory: dir.as_ref().to_path_buf(),
            file,
            mmap: None,
            needs_remap: false,
            head: ColorFragmentIndex(0),
            color_id_to_last_fragment_mapping: vec![ColorFragmentIndex(0)],
            delayed_writes: Vec::new(),
            generations: Generations::new(),
        })
    }

    pub fn load_or_new(dir: impl AsRef<Path>) -> Result<Self> {
        todo!()
    }

    #[inline]
    fn new_color_class_id(&self) -> ColorId {
        ColorId(self.color_id_to_last_fragment_mapping.len() as u32)
    }

    pub fn new_color_class(&mut self, color: u64) -> Result<ColorId> {
        // cannot add a color class outside of a generation
        if self.generations.current_generation().is_none() {
            return Err(ColorTableError::InvalidGenerationState);
        }

        let color_id = self.new_color_class_id();

        let fragment = ColorFragment {
            color,
            parent_pointer: ColorFragmentIndex(0),
        };

        self.file.write_all(fragment.as_bytes())?;

        self.head += 1;
        self.color_id_to_last_fragment_mapping.push(self.head);
        Ok(color_id)
    }

    pub fn start_generation(&mut self, generation: u64) -> Result<()> {
        self.generations
            .start_new_generation_at(self.head, generation)
    }

    pub fn end_generation(&mut self) -> Result<()> {
        self.generations.end_current_generation_at(self.head)?;

        // TODO: write deferred updates

        self.file.flush()?;
        Ok(())
    }

    #[inline]
    fn last_fragment_index(&self, color_id: &ColorId) -> Option<&ColorFragmentIndex> {
        self.color_id_to_last_fragment_mapping
            .get(color_id.0 as usize)
    }

    fn fragment(&self, idx: &ColorFragmentIndex) -> Option<&ColorFragment> {
        todo!();
    }

    #[inline]
    fn parent(&self, fragment: &ColorFragment) -> Option<&ColorFragment> {
        let ptr = fragment.parent_pointer;
        if ptr == ColorFragmentIndex(0) {
            None
        } else {
            self.fragment(&ptr)
        }
    }

    pub fn color_class(&self, color_id: &ColorId) -> Option<Vec<(u64, u64)>> {
        let next = self.last_fragment_index(color_id)?;
        let mut res = Vec::new();

        let mut frag = self.fragment(next)?;
        res.push((frag.color, *self.generations.find(next)?));

        while let Some(parent) = self.parent(frag) {
            frag = parent;
            let ptr = frag.parent_pointer; // packed field, must be copied to local to take a reference
            res.push((frag.color, *self.generations.find(&ptr)?));
        }

        Some(res)
    }
}
