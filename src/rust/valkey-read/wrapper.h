/* bindgen input: the C types the Rust read path reads and writes in place. */
#include "../../server.h"
#include "../../connection.h"
#include "../../io_threads.h"
#define USE_RUST_IO 1
#include "../../rust_read.h"
