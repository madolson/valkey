//! Rust port of the Valkey client read path (src/networking.c), built with
//! `make BUILD_RUST_IO=yes`. See read.rs for the function-by-function map.
#![allow(clippy::missing_safety_doc)]

#[allow(non_upper_case_globals, non_camel_case_types, non_snake_case, dead_code, clippy::all)]
pub mod sys {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

mod read;
mod sds;
