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
use std::ops::Deref;
use std::path::{Path, PathBuf};

use bincode::{Decode, Encode};
use bytemuck::{Pod, PodCastError, Zeroable};
use fs4::fs_std::FileExt;

use crate::generations::Generations;
use crate::{ColorTableConfig, ColorTableError, Result};

const TABLE_MAGIC: [u8; 12] = *b"CTBL\0\x00\x00\x01\0\0\0\0";

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

#[derive(Clone, Copy, Debug, Zeroable, Pod, Encode, Decode, Ord, PartialOrd, Eq, PartialEq)]
#[repr(transparent)]
pub struct ColorId(pub u32);

#[repr(C)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct ColorFragment {
    parent_pointer: ColorFragmentIndex, // or Option<NonZero<ColorFragmentIndex>> or something like that
    color: pack1::U64LE,
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
        // may fail if the bytes are misaligned
        bytemuck::from_bytes(bytes)
    }
}

/// Deferred update to a color class' tail fragment.
#[derive(Debug)]
struct DeferredUpdate {
    color_id: ColorId,
    parent: ColorFragmentIndex,
}

#[derive(Debug)]
struct ColorTableMmap {
    mmap: memmap2::Mmap,
    file: File,
}

impl ColorTableMmap {
    fn new(file: File) -> Result<Self> {
        if !FileExt::try_lock_shared(&file)? {
            // could not get file lock
            return Err(std::io::Error::from(std::io::ErrorKind::Deadlock).into());
        }
        // SAFETY: we hold a read lock on the file. this is not completely safe, but any well-behaved program should respect the lock.
        // if the file is modified while mmapped, UB
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file).unwrap() };

        Ok(Self { mmap, file })
    }

    fn remap(&mut self, new_len: usize) {
        todo!()
    }

    // may panic - seems to work fine even though mmap might be misaligned
    fn get_fragments(&self) -> &[ColorFragment] {
        bytemuck::cast_slice(&self.mmap)
    }

    fn get_fragment(&self, index: &ColorFragmentIndex) -> &ColorFragment {
        &self
            .get_fragments()
            .get(index.0 as usize)
            .expect("fragment not found")
    }

    fn try_get_fragments(&self) -> Option<&[ColorFragment]> {
        bytemuck::try_cast_slice(&self.mmap).ok()
    }

    fn try_get_fragment(&self, index: &ColorFragmentIndex) -> Option<&ColorFragment> {
        self.try_get_fragments()?.get(index.0 as usize)
    }
}

impl Drop for ColorTableMmap {
    fn drop(&mut self) {
        // unlock the file so it can be written to again
        let _ = FileExt::unlock(&self.file);
    }
}

impl Deref for ColorTableMmap {
    type Target = [ColorFragment];

    fn deref(&self) -> &Self::Target {
        self.get_fragments()
    }
}

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
            .truncate(true)
            .open(dir.as_ref().join(config.color_table_file_name))?;

        let mut file = BufWriter::with_capacity(config.buffer_size, file);
        // 12 bytes magic header to make offset calculations easier - maybe store len/format version/checksum later
        // if this is ever accessed as a fragment (idx 0), the result is valid but meaningless
        // currently not checked or validated
        file.write_all(&TABLE_MAGIC).unwrap();

        Ok(Self {
            directory: dir.as_ref().to_path_buf(),
            file,
            mmap: None,
            needs_remap: false,
            head: ColorFragmentIndex(1),
            color_id_to_last_fragment_mapping: vec![ColorFragmentIndex(0)],
            delayed_writes: Vec::new(),
            generations: Generations::new(),
        })
    }

    /// Write a fragment to the end of the file.
    ///
    /// Returns the index of the fragment.
    fn write_fragment(&mut self, fragment: &ColorFragment) -> Result<ColorFragmentIndex> {
        if self.is_mapped() {
            return Err(std::io::Error::from(std::io::ErrorKind::ResourceBusy).into());
        }
        let index = self.head;
        self.file.write_all(fragment.as_bytes())?;
        self.head += 1;
        Ok(index)
    }

    /// Maps the color table to memory.
    ///
    /// If the color table is already mapped, this is a no-op.
    pub fn map(&mut self) -> Result<()> {
        // reading while a generation is in progress will give incorrect results
        if self.generations.current_generation().is_some() {
            return Err(ColorTableError::InvalidGenerationState);
        }

        // maybe just error if it's already mapped
        if !self.is_mapped() {
            // try_clone() here is ~equivalent to dup(2), so the new fd points to the same file object (this is what we want)
            self.mmap = Some(ColorTableMmap::new(self.file.get_ref().try_clone()?)?);
        }

        Ok(())
    }

    fn get_map(&self) -> Result<&ColorTableMmap> {
        self.mmap.as_ref().ok_or(ColorTableError::NotMapped)
    }

    #[inline]
    const fn is_mapped(&self) -> bool {
        self.mmap.is_some()
    }

    pub fn unmap(&mut self) {
        self.mmap.take();
    }

    pub fn load_or_new(dir: impl AsRef<Path>) -> Result<Self> {
        todo!()
    }

    #[inline]
    const fn new_color_class_id(&self) -> ColorId {
        ColorId(self.color_id_to_last_fragment_mapping.len() as u32)
    }

    pub fn new_color_class(&mut self, color: u64) -> Result<ColorId> {
        // cannot add a color class outside of a generation
        if self.generations.current_generation().is_none() {
            return Err(ColorTableError::InvalidGenerationState);
        }

        let color_id = self.new_color_class_id();

        let fragment = ColorFragment {
            color: color.into(),
            parent_pointer: ColorFragmentIndex(0),
        };

        let fragment_idx = self.write_fragment(&fragment)?;
        self.color_id_to_last_fragment_mapping.push(fragment_idx);
        Ok(color_id)
    }

    /// Fork a color class.
    ///
    /// Returns the index of the new color class.
    /// You **MUST NOT** fork the returned color class until the next generation.
    pub fn fork_color_class(&mut self, parent: ColorId, color: u64) -> Result<ColorId> {
        if self.generations.current_generation().is_none() {
            return Err(ColorTableError::InvalidGenerationState);
        }

        let Some(parent_idx) = self.last_fragment_index(&parent) else {
            return Err(ColorTableError::InvalidColorId(parent.0));
        };

        let color_id = self.new_color_class_id();
        let fragment = ColorFragment {
            color: color.into(),
            parent_pointer: *parent_idx,
        };

        let fragment_idx = self.write_fragment(&fragment)?;
        self.color_id_to_last_fragment_mapping.push(fragment_idx);
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

    #[inline]
    fn fragment(&self, idx: &ColorFragmentIndex) -> Option<&ColorFragment> {
        assert!(self.is_mapped(), "color table must be mapped");
        if idx.0 == 0 {
            return None;
        }

        self.get_map().ok()?.try_get_fragment(idx)
    }

    #[inline]
    pub fn parent(&self, fragment: &ColorFragment) -> Option<&ColorFragment> {
        let ptr = &fragment.parent_pointer;
        if ptr == &ColorFragmentIndex(0) {
            None
        } else {
            self.fragment(ptr)
        }
    }

    pub fn color_class(&self, color_id: &ColorId) -> ClassIter {
        assert!(self.is_mapped(), "color table must be mapped");
        let idx = self
            .last_fragment_index(color_id)
            .unwrap_or(&ColorFragmentIndex(0)); // invalid color id will return an empty iterator

        ClassIter {
            color_table: self,
            idx,
        }
    }
}

#[derive(Debug)]
pub struct ClassIter<'c> {
    color_table: &'c ColorTable,
    idx: &'c ColorFragmentIndex,
}

// idk if this is bad
impl<'c> Iterator for ClassIter<'c> {
    type Item = (u64, u64); // color, generation

    fn next(&mut self) -> Option<Self::Item> {
        let frag = self.color_table.fragment(&self.idx)?;

        let res = (
            frag.color.get(),
            *self
                .color_table
                .generations
                .find(&self.idx)
                .expect("bug: missing generation"),
        );
        self.idx = &frag.parent_pointer;
        Some(res)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let lower = if self.idx == &ColorFragmentIndex(0) {
            0
        } else {
            1
        };

        let upper = self
            .color_table
            .generations
            .find(&self.idx)
            .map(|g| *g as usize + 1);
        (lower, upper)
    }
}
