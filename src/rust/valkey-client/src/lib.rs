//! Prototype for the `struct client` split. See design-docs/client-split.md.
#![allow(non_upper_case_globals, non_camel_case_types, non_snake_case, dead_code)]
#![allow(clippy::all)]

/// Shape 1: bindgen view of the real C `struct client` (server.h).
pub mod ffi {
    include!(concat!(env!("OUT_DIR"), "/server_bindings.rs"));
}

pub mod shape1;
pub mod shape2;
