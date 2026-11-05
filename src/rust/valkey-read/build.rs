// Generates Rust bindings for `struct client`, `struct valkeyServer` and the
// C functions the read path calls. The Makefile passes the server's own
// -I/-D flags in VALKEY_BINDGEN_CFLAGS so the struct layout matches.
use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=wrapper.h");
    for h in ["server.h", "connection.h", "sds.h", "io_threads.h"] {
        println!("cargo:rerun-if-changed=../../{h}");
    }
    println!("cargo:rerun-if-env-changed=VALKEY_BINDGEN_CFLAGS");

    let cflags = env::var("VALKEY_BINDGEN_CFLAGS").unwrap_or_default();
    let mut args: Vec<String> = cflags
        .split_whitespace()
        .filter(|a| a.starts_with("-D") || a.starts_with("-I") || a.starts_with("-std="))
        .map(|a| {
            // Include paths in FINAL_CFLAGS are relative to src/.
            if let Some(p) = a.strip_prefix("-I") {
                if !p.starts_with('/') {
                    return format!("-I../../{p}");
                }
            }
            a.to_string()
        })
        .collect();
    args.push("-I../..".to_string());

    let b = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_args(&args)
        .allowlist_type("client")
        .allowlist_type("valkeyServer")
        .allowlist_type("connection")
        .allowlist_var("server")
        .allowlist_var("SDS_NOINIT")
        .allowlist_var("READ_FLAGS_.*|PROTO_(IOBUF_LEN|INLINE_MAX_SIZE|MBULK_BIG_ARG|REQ_.*)|LL_.*")
        .allowlist_var("CONN_POSTPONE_.*|CONN_FLAG_POSTPONE_UPDATE_STATE|OBJ_STRING|REPL_DECODE_EVENT_BUDGET")
        .allowlist_var("CLIENT_TYPE_.*|C_OK|C_ERR")
        .allowlist_function("createObject")
        .allowlist_function("createStringObject")
        .allowlist_function("string2ll")
        .allowlist_function("valkey_malloc")
        .allowlist_function("valkey_realloc")
        .allowlist_function("valkey_free")
        .allowlist_function("sds(newlen|empty|range|clear|freesplitres|nsplitargs|MakeRoomFor|MakeRoomForNonGreedy|IncrLen|RemoveFreeSpace|free)")
        .allowlist_function("_serverLog")
        .allowlist_function("_serverAssert")
        .allowlist_function("_serverAssertWithInfo")
        .allowlist_function("_serverPanic")
        .allowlist_function("catClientInfoString")
        .allowlist_function("freeClientAsync")
        .allowlist_function("isReplicatedClient")
        .allowlist_function("getClientType")
        .allowlist_function("handleQbLimitReached")
        .allowlist_function("postponeClientRead")
        .allowlist_function("processInputBuffer")
        .allowlist_function("trimCommandQueue")
        .allowlist_function("beforeNextClient")
        .allowlist_function("replStreamHasPendingDecode")
        .allowlist_function("replDecodeToQueryBuf")
        .allowlist_function("readAndDecodePrimaryStream")
        .allowlist_function("inMainThread")
        .allowlist_function("isImportSlotMigrationJob")
        .allowlist_function("getCurTid")
        .allowlist_function("rustReadGetThreadSharedQb")
        .allowlist_function("rustReadSetThreadSharedQb")
        .derive_default(false)
        .derive_debug(false)
        .layout_tests(true)
        .generate_comments(false)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("bindgen failed on server.h");

    let out = PathBuf::from(env::var("OUT_DIR").unwrap()).join("bindings.rs");
    b.write_to_file(out).expect("write bindings");
}
