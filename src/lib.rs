//! color table!

#![cfg_attr(
    feature = "unstable_docs",
    feature(doc_auto_cfg),
    feature(doc_cfg_hide),
    doc(cfg_hide(doc))
)]
#![warn(clippy::unwrap_used)]

mod color_table;
pub use color_table::{ColorFragment, ColorFragmentIndex, ColorId, ColorTable};

pub(crate) mod generations;

#[cfg(any(feature = "roaring", doc))]
pub use ::roaring;
use thiserror::Error;
use typed_builder::TypedBuilder;

#[derive(Debug, Error)]
pub enum ColorTableError {
    #[error("saving color table failed")]
    Save,
    #[error("loading color table failed")]
    Load,
    #[error("I/O error")]
    Io(#[from] std::io::Error),
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
const FILE_NAME_HEAD_FRAGMENT_MAP: &str = "head_fragment_map";
const FILE_NAME_GENERATIONS: &str = "generations";

#[derive(Debug, Clone, TypedBuilder)]
pub struct ColorTableConfig {
    #[builder(setter(into), default = BUFFER_SIZE)]
    buffer_size: usize,
    #[builder(setter(into), default = String::from(FILE_NAME_COLOR_TABLE))]
    color_table_file_name: String,
    #[builder(
        setter(into),
        default = String::from(FILE_NAME_HEAD_FRAGMENT_MAP)
    )]
    head_fragment_map_file_name: String,
    #[builder(setter(into), default = String::from(FILE_NAME_GENERATIONS))]
    generations_file_name: String,
}

impl Default for ColorTableConfig {
    fn default() -> Self {
        ColorTableConfig::builder().build()
    }
}
