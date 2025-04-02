//! color table!

#![warn(/*missing_docs,*/ clippy::unwrap_used)]

mod color_table;
pub use color_table::{ColorFragment, ColorFragmentIndex, ColorId, ColorTable};

pub(crate) mod generations;

pub use roaring;
use thiserror::Error;
use typed_builder::TypedBuilder;

#[derive(Debug, Error)]
pub enum ColorTableError {
    #[error("loading color table failed")]
    Load,
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    // #[error("mmap error")] // mmap is also io basically
    // Mmap,
    #[error("invalid color id {0}")]
    InvalidColorId(u32),
    #[error("invalid generation {0}")]
    InvalidGeneration(u64),
    #[error("invalid generation state")]
    InvalidGenerationState,
    #[error("not mapped")]
    NotMapped,
}

type Result<T, E = ColorTableError> = std::result::Result<T, E>;

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

const BUFFER_SIZE: usize = 1 << 20; // 1 MiB

const FILE_NAME_COLOR_TABLE: &str = "color_table";
const FILE_NAME_LAST_COLOR_FRAGMENTS_MAPPING: &str = "last_color_fragments_mapping";
const FILE_NAME_GENERATION_RANGES: &str = "generation_ranges";

#[derive(Debug, TypedBuilder)]
pub struct ColorTableConfig {
    #[builder(setter(into), default = BUFFER_SIZE)]
    buffer_size: usize,
    #[builder(setter(into), default = String::from(FILE_NAME_COLOR_TABLE))]
    color_table_file_name: String,
    #[builder(
        setter(into),
        default = String::from(FILE_NAME_LAST_COLOR_FRAGMENTS_MAPPING)
    )]
    last_color_fragments_mapping_file_name: String,
    #[builder(setter(into), default = String::from(FILE_NAME_GENERATION_RANGES))]
    generation_ranges_file_name: String,
}

impl Default for ColorTableConfig {
    fn default() -> Self {
        ColorTableConfig::builder().build()
    }
}
