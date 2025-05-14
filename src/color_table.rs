//! color table!
//!
//! ### Terms:
//!
//! - color class: a set of color fragments. essentially one "color". analogous to a large bitvec/bitset
//!   - the null color class: a color class with id `0`, with no fragments
//!   - "head" fragment: the last fragment of a color class (no other fragments point to this fragment except for forks)
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
//!   - updating the "head" fragment of the color class is deferred until the next generation. this allows the index to be forked from the old "head" fragment until the next generation
//! - fragment indexes start at 1. fragment 0 is reserved as the parent of the "tail" fragment in a color class
//!   - each fragment can be found at offset `sizeof::<ColorFragment>() * fragment_index` in the color table file
//!
//! in our use case (kmer to sample mapping), the following is also true:
//! - for the set of kmers `K`, the mapping `K -> C` is surjective.
//!   - that is, each color class may correspond to multiple kmers, and every possible kmer maps to some color class (for the vast majority of kmers, this is the null color class)
//! - in order to save space, fragments are (read: should be) stored only if they contain at least one set bit

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use bincode::{Decode, Encode};
use bytemuck::{Pod, Zeroable};
use fs4::fs_std::FileExt;

use crate::generations::Generations;
use crate::{ColorTableConfig, ColorTableError, Result};

const TABLE_MAGIC: [u8; std::mem::size_of::<ColorFragment>()] = *b"CTBL\0\x00\x00\x01\0\0\0\0";

/// The index of a color fragment in the color table.
///
/// The fragment at index 0 is reserved as the parent of the "tail" fragment in a color class.
/// Real fragment indexes start at 1.
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

/// An identifier for a color class.
///
/// Each color class has a unique, immutable identifier that is used to refer to it.
/// The color class with id `0` is reserved as the null color class, and is empty.
#[derive(Clone, Copy, Debug, Zeroable, Pod, Encode, Decode, Ord, PartialOrd, Eq, PartialEq)]
#[repr(transparent)]
pub struct ColorId(pub u32);

/// A color fragment in the color table.
///
/// Each fragment in the color table contains a "partial color", representing 64 entries in a bitmap.
#[repr(C)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct ColorFragment {
    parent_pointer: ColorFragmentIndex, // or Option<NonZero<ColorFragmentIndex>> or something like that
    // unpadded u64
    color: pack1::U64LE,
}

/// Deferred update to a color class' "head" fragment.
#[derive(Debug)]
struct DeferredUpdate {
    color_id: ColorId,
    head: ColorFragmentIndex,
}

/// Wrapper around a memory-mapped color table file.
#[derive(Debug)]
struct ColorTableMmap {
    mmap: memmap2::Mmap,
    file: File,
}

impl ColorTableMmap {
    /// Create a new `ColorTableMmap` from the given file.
    ///
    /// Acquires a shared/read lock on the file.
    ///
    /// # Safety
    ///
    /// The file must not be modified while mmapped.
    ///
    /// # Errors
    ///
    /// Returns an error if the file could not be locked.
    unsafe fn new(file: File) -> Result<Self> {
        if !FileExt::try_lock_shared(&file)? {
            // could not get file lock
            return Err(io::Error::from(io::ErrorKind::ResourceBusy).into());
        }
        // SAFETY: we hold a read lock on the file. this is not completely safe, but any well-behaved program should respect the lock.
        // if the file is modified while mmapped, UB
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        #[cfg(unix)]
        mmap.advise(memmap2::Advice::Random)?; // we are reading the file backwards, so tell the OS not to read ahead

        Ok(Self { mmap, file })
    }

    // may panic in theory, but POSIX standards should guarantee that the memory is aligned to the page size (4KiB)
    #[inline]
    fn as_fragments(&self) -> &[ColorFragment] {
        bytemuck::cast_slice(&self.mmap)
    }

    fn get_fragment(&self, index: &ColorFragmentIndex) -> Option<&ColorFragment> {
        self.as_fragments().get(index.0 as usize)
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
        self.as_fragments()
    }
}

/// Compact on-disk bitmap storage.
#[derive(Debug)]
pub struct ColorTable {
    directory: PathBuf,
    config: ColorTableConfig,
    file: BufWriter<File>,
    mmap: Option<ColorTableMmap>,
    head: ColorFragmentIndex, // more or less file offset of last fragment
    color_id_head_fragment_map: Vec<ColorFragmentIndex>,
    deferred_updates: Vec<DeferredUpdate>,
    generations: Generations,
}

impl ColorTable {
    /// Creates a new `ColorTable` in the given directory.
    ///
    /// This method overwrites any existing files in the directory.
    /// If overwriting a previous color table is not desired, use [`ColorTable::load_or_new`].
    /// If you only want to load an existing color table, use [`ColorTable::load`].
    ///
    /// # Errors
    ///
    /// Returns an error if the color table file could not be created (e.g. if the directory does not exist).
    pub fn new(dir: impl AsRef<Path>, config: ColorTableConfig) -> Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.as_ref().join(&config.color_table_file_name))?;

        let mut file = BufWriter::with_capacity(config.buffer_size, file);
        // 12 bytes magic header to make offset calculations easier - maybe store len/format version/checksum later
        // if this is ever accessed as a fragment (idx 0), the result is valid but meaningless
        // currently not checked or validated
        file.write_all(&TABLE_MAGIC)?;

        Ok(Self {
            directory: dir.as_ref().to_path_buf(),
            config,
            file,
            mmap: None,
            // needs_remap: false,
            head: ColorFragmentIndex(1),
            color_id_head_fragment_map: vec![ColorFragmentIndex(0)],
            deferred_updates: Vec::new(),
            generations: Generations::new(),
        })
    }

    /// Loads an existing `ColorTable` from the given directory, or creates a new one if loading fails.
    ///
    /// # Errors
    ///
    /// Returns an error if both [`ColorTable::load`] and [`ColorTable::new`] fail.
    pub fn load_or_new(dir: impl AsRef<Path>, config: ColorTableConfig) -> Result<Self> {
        if let Ok(table) = Self::load(&dir, config.clone()) {
            return Ok(table);
        }

        Self::new(dir, config)
    }

    /// Loads an existing `ColorTable` from the given directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the color table files could not be opened (e.g. if the directory or file does not exist).
    pub fn load(dir: impl AsRef<Path>, config: ColorTableConfig) -> Result<Self> {
        let color_table = File::open(dir.as_ref().join(&config.color_table_file_name))?;
        let ct_size = color_table.metadata()?.len();
        if ct_size % std::mem::size_of::<ColorFragment>() as u64 != 0 {
            return Err(io::Error::from(io::ErrorKind::InvalidData).into());
        }

        let head =
            ColorFragmentIndex((ct_size / std::mem::size_of::<ColorFragment>() as u64) as u32);

        let mut generations_reader = io::BufReader::new(File::open(
            dir.as_ref().join(&config.generations_file_name),
        )?);
        let generations: Generations =
            bincode::decode_from_std_read(&mut generations_reader, crate::BINCODE_CONFIG)
                .map_err(|_| ColorTableError::Load)?;

        let mut fragment_map_reader = io::BufReader::new(File::open(
            dir.as_ref().join(&config.head_fragment_map_file_name),
        )?);
        let fragment_map: Vec<ColorFragmentIndex> =
            bincode::decode_from_std_read(&mut fragment_map_reader, crate::BINCODE_CONFIG)
                .map_err(|_| ColorTableError::Load)?;

        if fragment_map.is_empty() {
            return Err(io::Error::from(io::ErrorKind::InvalidData).into());
        }

        // copy
        let buffer_size = config.buffer_size;

        Ok(Self {
            directory: dir.as_ref().to_path_buf(),
            config,
            file: BufWriter::with_capacity(buffer_size, color_table),
            mmap: None,
            // needs_remap: false,
            head,
            color_id_head_fragment_map: fragment_map,
            deferred_updates: Vec::new(),
            generations,
        })
    }

    /// Syncs the color table to disk.
    ///
    /// This method overwrites any existing files in the directory.
    /// You may provide a [`ColorTableConfig`] to control where the files are saved.
    /// If no config is provided, the config that was used to create the color table is used.
    ///
    /// # Errors
    ///
    /// Returns an error if the color table is currently mmapped, or if the color table files could not be updated.
    // maybe want to take config as an argument to avoid storing it in the struct
    pub fn sync(&mut self, config: Option<&ColorTableConfig>) -> Result<()> {
        // if mmapped, don't sync
        // alternatively, unmap before syncing
        if self.is_mapped() {
            return Err(io::Error::from(io::ErrorKind::ResourceBusy).into());
        }

        let config = config.unwrap_or(&self.config);

        // sync table to disk
        self.file.flush()?;

        let mut generations_writer = io::BufWriter::new(File::create(
            self.directory.join(&config.generations_file_name),
        )?);
        bincode::encode_into_std_write(
            &self.generations,
            &mut generations_writer,
            crate::BINCODE_CONFIG,
        )
        .map_err(|_| ColorTableError::Save)?;

        let mut fragment_map_writer = io::BufWriter::new(File::create(
            self.directory.join(&config.head_fragment_map_file_name),
        )?);
        bincode::encode_into_std_write(
            &self.color_id_head_fragment_map,
            &mut fragment_map_writer,
            crate::BINCODE_CONFIG,
        )
        .map_err(|_| ColorTableError::Save)?;

        Ok(())
    }

    /// Maps the color table to memory.
    ///
    /// If the color table is already mapped, this is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error if a generation is in progress or if mmapping fails.
    pub fn map(&mut self) -> Result<()> {
        // reading while a generation is in progress will give incorrect results
        if self.generations.current_generation().is_some() {
            return Err(ColorTableError::InvalidGenerationState);
        }

        // maybe just error if it's already mapped
        if !self.is_mapped() {
            // sync to disk
            self.file.flush()?;
            // try_clone() here is ~equivalent to dup(2), so the new fd points to the same file object (this is what we want)
            // SAFETY: `Self` will not modify the file while it is mmapped
            self.mmap = Some(unsafe { ColorTableMmap::new(self.file.get_ref().try_clone()?) }?);
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

    /// Unmaps the color table from memory.
    pub fn unmap(&mut self) {
        self.mmap.take();
    }

    /// Write a fragment to the end of the file.
    ///
    /// Returns the index of the fragment.
    ///
    /// # Errors
    ///
    /// Returns an error if the color table is currently mmapped or if the color table file could not be updated.
    fn write_fragment(&mut self, fragment: ColorFragment) -> Result<ColorFragmentIndex> {
        if self.is_mapped() {
            return Err(io::Error::from(io::ErrorKind::ResourceBusy).into());
        }
        let index = self.head;
        let bytes = bytemuck::bytes_of(&fragment);
        self.file.write_all(bytes.as_ref())?;
        self.head += 1;
        Ok(index)
    }

    #[inline]
    fn new_color_class_id(&self) -> ColorId {
        ColorId(self.color_id_head_fragment_map.len() as u32)
    }

    /// Creates a new color class.
    ///
    /// Returns the index of the new color class.
    /// You **MUST NOT** fork or extend the returned color class until the next generation.
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

        let fragment_idx = self.write_fragment(fragment)?;
        self.color_id_head_fragment_map.push(fragment_idx);
        Ok(color_id)
    }

    /// Fork a color class.
    ///
    /// Returns the index of the new color class.
    /// You **MUST NOT** fork or extend the returned color class until the next generation.
    pub fn fork_color_class(&mut self, parent: ColorId, color: u64) -> Result<ColorId> {
        if self.generations.current_generation().is_none() {
            return Err(ColorTableError::InvalidGenerationState);
        }

        let Some(parent_idx) = self.head_fragment_index(&parent) else {
            return Err(ColorTableError::InvalidColorId(parent.0));
        };

        let color_id = self.new_color_class_id();
        let fragment = ColorFragment {
            color: color.into(),
            parent_pointer: *parent_idx,
        };

        let fragment_idx = self.write_fragment(fragment)?;
        self.color_id_head_fragment_map.push(fragment_idx);
        Ok(color_id)
    }

    /// Extend a color class.
    ///
    /// You **MUST NOT** extend the color class again until the next generation.
    /// You may fork the color class after extending it within the same generation.
    pub fn extend_color_class(&mut self, parent: ColorId, color: u64) -> Result<()> {
        if self.generations.current_generation().is_none() {
            return Err(ColorTableError::InvalidGenerationState);
        }

        let Some(parent_idx) = self.head_fragment_index(&parent) else {
            return Err(ColorTableError::InvalidColorId(parent.0));
        };

        let fragment = ColorFragment {
            color: color.into(),
            parent_pointer: *parent_idx,
        };

        let fragment_idx = self.write_fragment(fragment)?;

        let delayed_write = DeferredUpdate {
            color_id: parent,
            head: fragment_idx,
        };

        self.deferred_updates.push(delayed_write);
        Ok(())
    }

    /// Start a new generation.
    ///
    /// The new generation number must be greater than the last generation.
    /// The current generation must be ended before starting a new one.
    pub fn start_generation(&mut self, generation: u64) -> Result<()> {
        self.generations
            .start_new_generation_at(self.head, generation)
    }

    /// End the current generation.
    ///
    /// This will additionally flush the color table to disk.
    pub fn end_generation(&mut self) -> Result<()> {
        self.generations.end_current_generation_at(self.head)?;

        // sorting is useful for cache locality? maybe?
        self.deferred_updates
            .sort_unstable_by_key(|update| update.color_id);

        // borrow check trolling me
        for update in self.deferred_updates.drain(..) {
            let color_id = update.color_id;
            self.color_id_head_fragment_map
                .get_mut(color_id.0 as usize)
                .map(|v| *v = update.head)
                .ok_or(ColorTableError::InvalidColorId(color_id.0))?;
        }

        self.file.flush()?;

        Ok(())
    }

    #[inline]
    fn head_fragment_index(&self, color_id: &ColorId) -> Option<&ColorFragmentIndex> {
        self.color_id_head_fragment_map.get(color_id.0 as usize)
    }

    #[inline]
    fn fragment(&self, idx: &ColorFragmentIndex) -> Option<&ColorFragment> {
        assert!(self.is_mapped(), "color table must be mapped");
        if idx.0 == 0 {
            return None;
        }

        self.get_map().ok()?.get_fragment(idx)
    }

    /// Get the parent fragment of the given fragment, if it exists.
    #[inline]
    pub fn parent_of(&self, fragment: &ColorFragment) -> Option<&ColorFragment> {
        let ptr = &fragment.parent_pointer;
        if ptr == &ColorFragmentIndex(0) {
            None
        } else {
            self.fragment(ptr)
        }
    }

    /// Get an iterator over the color class referred to by the given color id.
    ///
    /// Iterator items are `(partial color, generation)` pairs.
    pub fn color_class(&self, color_id: &ColorId) -> ClassIter {
        assert!(self.is_mapped(), "color table must be mapped");
        let idx = self
            .head_fragment_index(color_id)
            .unwrap_or(&ColorFragmentIndex(0)); // invalid color id will return an empty iterator

        ClassIter {
            color_table: self,
            idx,
        }
    }
}

impl Drop for ColorTable {
    fn drop(&mut self) {
        // if this is run before unmap, it may block/error
        let _ = self.sync(None);
    }
}

/// Iterator over a color class.
#[derive(Debug)]
pub struct ClassIter<'c> {
    color_table: &'c ColorTable,
    idx: &'c ColorFragmentIndex,
}

impl<'c> ClassIter<'c> {
    /// Convert the iterator into a roaring bitmap.
    #[cfg(any(feature = "roaring", doc))]
    pub fn into_bitmap(self) -> roaring::RoaringBitmap {
        let mut bitmap = roaring::RoaringBitmap::new();
        for (color, _gen) in self {
            let gen_offset = _gen
                .checked_mul(u64::BITS as u64)
                .expect("generation overflow");

            let bits = bitfrob::U64BitIterLow::from_count_and_bits(1, color)
                .enumerate()
                .filter_map(|(n, b)| {
                    if b == 1 {
                        Some(n as u32 + gen_offset as u32)
                    } else {
                        None
                    }
                });

            bitmap.extend(bits);
        }
        bitmap
    }
}

// idk if this is bad
impl<'c> Iterator for ClassIter<'c> {
    type Item = (u64, u64); // color, generation

    fn next(&mut self) -> Option<Self::Item> {
        let frag = self.color_table.fragment(self.idx)?;

        let res = (
            frag.color.get(),
            *self
                .color_table
                .generations
                .find(self.idx)
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
            .find(self.idx)
            .map(|g| *g as usize + 1);
        (lower, upper)
    }
}
