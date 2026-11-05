// Shape 1 (shared struct): generate a bindgen view of the real `struct client`
// from server.h, so Rust reads and writes its fields in place.
use std::env;
use std::path::PathBuf;

fn main() {
    let src = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap()).join("../..");
    let src = src.canonicalize().unwrap();
    let deps = src.join("../deps");
    println!("cargo:rerun-if-changed=wrapper.h");
    for h in ["server.h", "connection.h", "sds.h", "client_io.h"] {
        println!("cargo:rerun-if-changed={}", src.join(h).display());
    }
    let mut b = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_arg("-std=gnu11")
        .clang_arg("-DUSE_JEMALLOC")
        .clang_arg(format!("-I{}", src.display()));
    for d in ["libvalkey/include", "hdr_histogram", "fpconv", "fast_float", "lz4", "jemalloc/include", "lua/src"] {
        b = b.clang_arg(format!("-I{}", deps.join(d).display()));
    }
    let bindings = b
        .allowlist_type("client")
        .allowlist_type("connection")
        .allowlist_type("ConnectionType")
        .allowlist_function("parseInlineBuffer")
        .allowlist_function("parseMultibulkBuffer")
        .allowlist_function("_serverPanic")
        .allowlist_var("CONN_POSTPONE_.*")
        .allowlist_var("CONN_FLAG_POSTPONE_UPDATE_STATE")
        .allowlist_var("PROTO_REQ_.*")
        .allowlist_type("clientIOState")
        .rustified_enum("clientIOState")
        .allowlist_function("_serverAssert")
        .opaque_type("user")
        // Shape 2: ClientIoRead is defined in Rust; bindgen must reuse it so
        // the layout tests check the embedded copy against the C header.
        .blocklist_type("ClientIoRead")
        .raw_line("pub use crate::shape2::ClientIoRead;")
        .opaque_type("serverCommand")
        .derive_default(false)
        .layout_tests(true)
        .generate()
        .expect("bindgen failed on server.h");
    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings.write_to_file(out.join("server_bindings.rs")).unwrap();
}
