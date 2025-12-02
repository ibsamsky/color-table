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
//!
//! Gilbert's notes derived from what Isaac said:
//! - The index of genome data is made of both a CQF and a color table. The CQF is used for
//!   membership queries and provides `ColorId`s (stored in the "count" field of a CQF) for each
//!   k-mer. The `ColorId`s are then passed to a color table, which provides which samples (plural)
//!   the k-mer is found in.
//! - Summary: CQF = `HashMap<Kmer, ColorId>`, ColorTable = `HashMap<ColorId, BitVec<Sample>>`.
//!   Together, they form a colored de Bruijn graph (?).

use std::fs::File;
use std::io::{self, BufWriter, Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use bincode::{Decode, Encode};
use bytemuck::{Pod, Zeroable};
use parking_lot::{Mutex, RwLock};

cfg_if::cfg_if! {
    if #[cfg(feature = "typesize")] {
        use typesize::TypeSize;
        use typesize::derive::TypeSize;
    }
}

use crate::generations::Generations;
use crate::{ColorTableConfig, ColorTableError, Result};

const TABLE_MAGIC: [u8; std::mem::size_of::<ColorFragment>()] = *b"CTBL\0\x00\x00\x01";

/// The index of a color fragment in the color table.
///
/// The fragment at index 0 is reserved as the parent of the "tail" fragment in a color class.
/// Real fragment indexes start at 1.
#[derive(Clone, Copy, Debug, Zeroable, Pod, Encode, Decode, Ord, PartialOrd, Eq, PartialEq)]
#[cfg_attr(feature = "typesize", derive(TypeSize))]
#[repr(transparent)]
pub struct ColorFragmentIndex(pub u32); // up to 4b fragments/colors

impl std::ops::Add<u32> for ColorFragmentIndex {
    type Output = Self;

    #[inline]
    #[track_caller]
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
    #[track_caller]
    fn add_assign(&mut self, other: u32) {
        let res = if cfg!(debug_assertions) {
            self.0.checked_add(other).expect("overflow")
        } else {
            self.0.wrapping_add(other)
        };

        self.0 = res;
    }
}

impl From<ColorId> for ColorFragmentIndex {
    #[inline]
    fn from(id: ColorId) -> Self {
        ColorFragmentIndex(id.0)
    }
}

impl From<&ColorId> for ColorFragmentIndex {
    #[inline]
    fn from(id: &ColorId) -> Self {
        ColorFragmentIndex(id.0)
    }
}

/// An identifier for a color class.
///
/// Each color class has a unique, immutable identifier that is used to refer to it.
/// The color class with id `0` is reserved as the null color class, and is empty.
#[derive(
    Clone, Copy, Debug, Zeroable, Pod, Encode, Decode, Hash, Ord, PartialOrd, Eq, PartialEq,
)]
#[cfg_attr(feature = "typesize", derive(TypeSize))]
#[repr(transparent)]
pub struct ColorId(pub(crate) u32);

impl ColorId {
    /// Create a new `ColorId` from the given u32 value.
    #[inline]
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    /// Get the underlying u32 value (file offset) of the color ID.
    #[inline]
    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl From<ColorFragmentIndex> for ColorId {
    #[inline]
    fn from(idx: ColorFragmentIndex) -> Self {
        ColorId(idx.0)
    }
}

impl From<&ColorFragmentIndex> for ColorId {
    #[inline]
    fn from(idx: &ColorFragmentIndex) -> Self {
        ColorId(idx.0)
    }
}

/// A color fragment in the color table.
///
/// Each fragment in the color table contains a "partial color", representing 64 entries in a bitmap.
#[repr(C)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct ColorFragment {
    parent_pointer: ColorFragmentIndex, // or Option<NonZero<ColorFragmentIndex>> or something like that
    // unpadded u32
    color: pack1::U32LE,
}

/// Wrapper around a memory-mapped color table file.
#[derive(Debug)]
struct ColorTableMmap {
    mmap: memmap2::Mmap,
}

#[cfg(feature = "typesize")]
impl TypeSize for ColorTableMmap {
    fn extra_size(&self) -> usize {
        self.mmap.len()
    }
}

impl ColorTableMmap {
    /// Create a new `ColorTableMmap` from the given file.
    ///
    /// # Safety
    ///
    /// The file must not be modified while mmapped.
    ///
    /// # Errors
    ///
    /// Returns an error if the file could not be mmapped.
    unsafe fn new(file: File) -> Result<Self> {
        // SAFETY: the caller must ensure that the file is not modified.
        // we never modify the part of the file that is mmapped; we only append to the file, which should not cause any issues.
        // if the file is truncated (by another process) while mmapped, kernel will send SIGBUS on access
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        #[cfg(unix)]
        mmap.advise(memmap2::Advice::Random)?; // we are reading the file backwards, so tell the OS not to read ahead

        Ok(Self { mmap })
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
    config: Box<ColorTableConfig>,
    // buffered writer for the color table file, and current head index
    // the head index is only modified while holding the lock, so it stays in sync with the file
    file: Mutex<(BufWriter<File>, ColorFragmentIndex)>,

    generation_lock: Mutex<()>,
    generations: RwLock<Generations>,
}

#[cfg(feature = "typesize")]
impl TypeSize for ColorTable {
    fn extra_size(&self) -> usize {
        self.directory.capacity()
            + self.config.extra_size()
            + self.file.lock().0.capacity()
            + (40 * (std::mem::size_of::<ColorFragmentIndex>() + std::mem::size_of::<(u64, u64)>()))
    }
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
            config: Box::new(config),
            file: Mutex::new((file, ColorFragmentIndex(1))),
            generation_lock: Mutex::new(()),
            generations: RwLock::new(Generations::new()),
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
        let mut color_table = File::options()
            .read(true)
            .append(true)
            .open(dir.as_ref().join(&config.color_table_file_name))?;
        let ct_size = color_table.metadata()?.len();
        if !ct_size.is_multiple_of(std::mem::size_of::<ColorFragment>() as u64) {
            return Err(io::Error::from(io::ErrorKind::InvalidData).into());
        }

        // check magic header
        let mut buf = [0; std::mem::size_of::<ColorFragment>()];
        color_table.read_exact(&mut buf)?;

        if buf != TABLE_MAGIC {
            // file was probably truncated or corrupted
            return Err(io::Error::from(io::ErrorKind::InvalidData).into());
        }

        let head =
            ColorFragmentIndex((ct_size / std::mem::size_of::<ColorFragment>() as u64) as u32);

        let mut generations_reader = io::BufReader::new(File::open(
            dir.as_ref().join(&config.generations_file_name),
        )?);
        let generations: RwLock<Generations> = RwLock::new(bincode::decode_from_std_read(
            &mut generations_reader,
            crate::BINCODE_CONFIG,
        )?);

        // copy
        let buffer_size = config.buffer_size;

        Ok(Self {
            directory: dir.as_ref().to_path_buf(),
            config: Box::new(config),
            file: Mutex::new((BufWriter::with_capacity(buffer_size, color_table), head)),
            generation_lock: Mutex::new(()),
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
    pub fn sync(&self, config: Option<&ColorTableConfig>) -> Result<()> {
        let config = config.unwrap_or(&self.config);

        // sync table to disk
        self.file.lock().0.flush()?;

        let mut generations_writer = io::BufWriter::new(File::create(
            self.directory.join(&config.generations_file_name),
        )?);
        bincode::encode_into_std_write(
            self.generations.read().deref(),
            &mut generations_writer,
            crate::BINCODE_CONFIG,
        )?;

        Ok(())
    }

    /// Maps the color table to memory.
    ///
    /// # Errors
    ///
    /// Returns an error if mmapping fails.
    pub fn map(&self) -> Result<MmapGuard<'_>> {
        // sync to disk
        self.file.lock().0.flush()?;

        // try_clone() here is ~equivalent to dup(2), so the new fd points to the same file object (this is what we want)
        // SAFETY: `Self` will not modify the file while it is mmapped
        let mmap = unsafe { ColorTableMmap::new(self.file.lock().0.get_ref().try_clone()?) }?;

        Ok(MmapGuard(self, mmap))
    }

    /// Write a fragment to the end of the file.
    ///
    /// Returns the index of the fragment.
    ///
    /// # Errors
    ///
    /// Returns an error if the color table is currently mmapped or if the color table file could not be updated.
    #[inline]
    fn write_fragment(&self, fragment: ColorFragment) -> Result<ColorFragmentIndex> {
        let index = {
            let mut guard = self.file.lock();
            let index = guard.1;
            let bytes = bytemuck::bytes_of(&fragment);
            guard.0.write_all(bytes.as_ref())?;
            guard.1 += 1;
            index
        };

        Ok(index)
    }

    /// Perform an operation within a new generation.
    ///
    /// The new generation number must be greater than the last generation.
    /// The generation will automatically be ended when the provided closure returns.
    ///
    /// If this function is called while another generation is in progress, it will block until the other generation has ended.
    ///
    /// The color table can still be queried while a generation is in progress, but no changes will be visible until the generation is ended.
    pub fn with_generation<R>(
        &self,
        generation: u64,
        f: impl FnOnce(GenerationGuard<'_>) -> R,
    ) -> Result<R> {
        let _guard = self.generation_lock.lock();
        self.generations
            .write()
            .start_new_generation_at(self.file.lock().1, generation)?;

        // run the closure
        let res = f(GenerationGuard { table: self });

        self.generations
            .write()
            .end_current_generation_at(self.file.lock().1)?;

        self.file.lock().0.flush()?;

        Ok(res)
    }

    #[inline]
    fn head_fragment_index(&self, color_id: &ColorId) -> Option<ColorFragmentIndex> {
        if color_id.0 < self.file.lock().1.0 {
            Some(color_id.into())
        } else {
            None
        }
    }
}

pub struct GenerationGuard<'a> {
    table: &'a ColorTable,
}

impl<'a> GenerationGuard<'a> {
    /// Creates a new color class.
    ///
    /// Returns the index of the new color class.
    /// You **MUST NOT** fork or extend the returned color class until the next generation.
    pub fn new_color_class(&self, color: u32) -> Result<ColorId> {
        let fragment = ColorFragment {
            color: color.into(),
            parent_pointer: ColorFragmentIndex(0),
        };

        let color_id = self.table.write_fragment(fragment)?.into();

        Ok(color_id)
    }

    /// Fork a color class.
    ///
    /// Returns the index of the new color class.
    /// You **MUST NOT** fork or extend the returned color class until the next generation.
    #[must_use = "`parent` is not modified; you must use the returned `ColorId` to refer to the forked color class"]
    pub fn fork_color_class(&self, parent: ColorId, color: u32) -> Result<ColorId> {
        let Some(parent_idx) = self.table.head_fragment_index(&parent) else {
            return Err(ColorTableError::InvalidColorId(parent.0));
        };

        let fragment = ColorFragment {
            color: color.into(),
            parent_pointer: parent_idx,
        };

        let color_id = self.table.write_fragment(fragment)?.into();

        Ok(color_id)
    }

    /// Extend a color class.
    ///
    /// You **MUST NOT** extend the color class again until the next generation.
    /// You may fork the color class after extending it within the same generation using the
    /// *original* [`ColorId`] (passed as `parent`).
    #[must_use = "`parent` is not modified; you must use the returned `ColorId` to refer to the extended color class"]
    pub fn extend_color_class(&self, parent: ColorId, color: u32) -> Result<ColorId> {
        let Some(parent_idx) = self.table.head_fragment_index(&parent) else {
            return Err(ColorTableError::InvalidColorId(parent.0));
        };

        let fragment = ColorFragment {
            color: color.into(),
            parent_pointer: parent_idx,
        };

        let color_id = self.table.write_fragment(fragment)?.into();

        Ok(color_id)
    }
}

/// RAII guard for a memory-mapped color table.
#[derive(Debug)]
pub struct MmapGuard<'a>(&'a ColorTable, ColorTableMmap);

impl<'a> MmapGuard<'a> {
    /// Get a reference to the color table.
    #[inline]
    pub fn color_table(&self) -> &ColorTable {
        self.0
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

    #[inline]
    fn fragment(&self, idx: &ColorFragmentIndex) -> Option<&ColorFragment> {
        if idx.0 == 0 {
            return None;
        }

        self.1.get_fragment(idx)
    }

    /// Get an iterator over the color class referred to by the given color id.
    ///
    /// Iterator items are `(partial color, generation)` pairs. The order in which pairs are yielded
    /// is unspecified. Results may be stale if a generation is in progress.
    pub fn color_class(&self, color_id: &ColorId) -> ClassIter<'_> {
        let idx = self
            .0
            .head_fragment_index(color_id)
            .unwrap_or(ColorFragmentIndex(0)); // invalid color id will return an empty iterator

        ClassIter { map: self, idx }
    }
}

impl Drop for ColorTable {
    fn drop(&mut self) {
        let _ = self.sync(None);
    }
}

/// Iterator over a color class.
#[derive(Debug)]
pub struct ClassIter<'c> {
    map: &'c MmapGuard<'c>,
    idx: ColorFragmentIndex,
}

impl<'c> ClassIter<'c> {
    /// Convert the iterator into a roaring bitmap.
    #[cfg(feature = "roaring")]
    pub fn into_bitmap(self) -> roaring::RoaringBitmap {
        let mut bitmap = roaring::RoaringBitmap::new();
        let mut set = self.into_indices();
        set.sort_unstable();
        bitmap.extend(set.into_iter().map(|i| i as u32));
        bitmap
    }

    /// Convert the iterator into a vector of indices.
    ///
    /// Indices are NOT sorted.
    pub fn into_indices(self) -> Vec<usize> {
        #[inline]
        fn decode_bitmap(buf: &mut Vec<usize>, mut bm: u32, k: u64) {
            while bm != 0 {
                let low = bm & bm.wrapping_neg();
                let idx = bm.trailing_zeros() as u64;
                buf.push((k * std::mem::size_of_val(&bm) as u64 * 8 + idx) as usize);
                bm ^= low;
            }
        }

        let mut indices = if let Some(len) = self.size_hint().1 {
            Vec::with_capacity(len * 32) // reasonable estimate; in normal usage this will take about 15 kB at most
        } else {
            Vec::new()
        };

        for (color, gen_) in self {
            decode_bitmap(&mut indices, color, gen_);
        }

        indices
    }
}

// idk if this is bad
impl<'c> Iterator for ClassIter<'c> {
    type Item = (u32, u64); // color, generation

    fn next(&mut self) -> Option<Self::Item> {
        let frag = self.map.fragment(&self.idx)?;

        let res = (
            frag.color.get(),
            *self
                .map
                .color_table()
                .generations
                .read()
                .find(&self.idx)
                .expect("bug: missing generation"),
        );
        self.idx = frag.parent_pointer;
        Some(res)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let lower = if self.idx == ColorFragmentIndex(0) {
            0
        } else {
            1
        };

        let upper = self
            .map
            .color_table()
            .generations
            .read()
            .find(&self.idx)
            .map(|g| *g as usize + 1);
        (lower, upper)
    }
}
