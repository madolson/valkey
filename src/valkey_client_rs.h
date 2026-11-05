/* Functions exported by the Rust client-split prototype
 * (src/rust/valkey-client). Built with BUILD_RUST_CLIENT=yes. */
#ifndef VALKEY_CLIENT_RS_H
#define VALKEY_CLIENT_RS_H

struct client;

/* Shape 1: beat-for-beat port of parseInputBuffer() over the bindgen view. */
void vc_parse_input_buffer(struct client *c);

#endif
