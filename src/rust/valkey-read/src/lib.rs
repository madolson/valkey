//! Rust port of the Valkey client read path (src/networking.c), built with
//! `make BUILD_RUST_IO=yes`. See read.rs for the function-by-function map.
#![allow(clippy::missing_safety_doc)]

// bindings.rs is generated for this target only (`make rust-bindings`).
#[cfg(not(all(target_arch = "x86_64", target_os = "linux")))]
compile_error!("valkey-read: src/bindings.rs is generated for x86_64 Linux only");

#[allow(
    non_upper_case_globals,
    non_camel_case_types,
    non_snake_case,
    dead_code,
    clippy::all
)]
pub mod sys {
    // Checked in; regenerate with `make rust-bindings`.
    include!("bindings.rs");
}

mod read;
mod sds;
