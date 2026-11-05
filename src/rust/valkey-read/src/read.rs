//! Beat-for-beat port of the client read path in src/networking.c.
//!
//! Every function here mirrors one C function of the same name; the C body is
//! compiled out with `#ifndef USE_RUST_IO` and this crate exports the symbol
//! instead. Fields of `client` and `server` are read and written in place
//! through bindgen's layout of src/server.h. Transport access goes only
//! through the `ConnectionType` vtable (`connRead`, `connGetState`,
//! `connGetLastError`), so socket, Unix, TLS and RDMA connections all work.
//! Anything that creates `robj`s, logs, or frees clients calls back into C.

use crate::sds::{sdsalloc, sdsavail, sdslen};
use crate::sys::*;
use core::ffi::{c_char, c_int, c_void};
use core::ptr::{self, null, null_mut};
use core::sync::atomic::{AtomicI64, Ordering};

extern "C" {
    fn memchr(s: *const c_void, c: c_int, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

use crate::sys::{valkey_free as zfree, valkey_malloc as zmalloc, valkey_realloc as zrealloc};

/// `c->flag` (an anonymous union with raw_flag1/raw_flag2 in server.h).
#[inline(always)]
unsafe fn flag<'a>(c: *mut client) -> &'a ClientFlags {
    &(*c).__bindgen_anon_1.flag
}

const INT_MAX: c_int = c_int::MAX;
/// networking.c `COMMAND_QUEUE_MIN_CAPACITY`
const COMMAND_QUEUE_MIN_CAPACITY: u16 = 16;
/// networking.c `REPL_MAX_READS_PER_IO_EVENT`
const REPL_MAX_READS_PER_IO_EVENT: c_int = 25;
const CLIENT_IDLE: u8 = 0;

/// serverAssertWithInfo() / serverAssert() src/server.h
macro_rules! server_assert {
    ($c:expr, $e:expr) => {
        if !($e) {
            _serverAssertWithInfo(
                $c,
                null(),
                concat!(stringify!($e), "\0").as_ptr() as *const c_char,
                concat!(file!(), "\0").as_ptr() as *const c_char,
                line!() as c_int,
            );
            std::process::abort();
        }
    };
}

/// serverLog() src/server.h: check the level before evaluating arguments.
macro_rules! server_log {
    ($level:expr, $fmt:expr $(, $arg:expr)*) => {
        if (($level as c_int) & 0xff) >= server.verbosity {
            _serverLog($level as c_int, concat!($fmt, "\0").as_ptr() as *const c_char $(, $arg)*);
        }
    };
}

#[inline(always)]
unsafe fn thread_shared_qb() -> sds {
    rustReadGetThreadSharedQb()
}

#[inline(always)]
unsafe fn unixtime() -> i64 {
    // server.unixtime is _Atomic(time_t).
    (*(ptr::addr_of_mut!(server.unixtime) as *const AtomicI64)).load(Ordering::SeqCst)
}

#[inline(always)]
unsafe fn io_read_state(c: *mut client) -> u8 {
    ptr::read_volatile(ptr::addr_of!((*c).io_read_state))
}

#[inline(always)]
unsafe fn io_write_state(c: *mut client) -> u8 {
    ptr::read_volatile(ptr::addr_of!((*c).io_write_state))
}

/// connRead() src/connection.h
#[inline(always)]
unsafe fn conn_read(conn: *mut connection, buf: *mut c_void, len: usize) -> c_int {
    ((*(*conn).type_).read.unwrap_unchecked())(conn, buf, len)
}

/// connGetState() src/connection.h
#[inline(always)]
unsafe fn conn_get_state(conn: *mut connection) -> c_int {
    (*conn).state as c_int
}

/// connGetLastError() src/connection.h
#[inline(always)]
unsafe fn conn_get_last_error(conn: *mut connection) -> *const c_char {
    ((*(*conn).type_).get_last_error.unwrap_unchecked())(conn)
}

/// connSetPostponeUpdateState() src/connection.h
#[inline(always)]
unsafe fn conn_set_postpone_update_state(conn: *mut connection, postpone_mask: c_int) {
    if !conn.is_null() {
        if postpone_mask != 0 {
            (*conn).flags |= CONN_FLAG_POSTPONE_UPDATE_STATE as i16;
        } else {
            (*conn).flags &= !(CONN_FLAG_POSTPONE_UPDATE_STATE as i16);
        }
        if !(*conn).type_.is_null() {
            if let Some(f) = (*(*conn).type_).postpone_update_state {
                f(conn, postpone_mask);
            }
        }
    }
}

/// clientConnPostponeMaskFromIOState() src/server.h
#[inline(always)]
unsafe fn client_conn_postpone_mask_from_io_state(c: *mut client) -> c_int {
    let mut mask = 0;
    if io_read_state(c) != CLIENT_IDLE {
        mask |= CONN_POSTPONE_READ as c_int;
    }
    if io_write_state(c) != CLIENT_IDLE {
        mask |= CONN_POSTPONE_WRITE as c_int;
    }
    if flag(c).forkless_managed() != 0 {
        mask |= CONN_POSTPONE_WRITE as c_int;
    }
    mask
}

/// getClientType() src/server.h (static inline there).
unsafe fn get_client_type(c: *mut client) -> c_int {
    let f = flag(c);
    if f.primary() != 0 {
        return CLIENT_TYPE_PRIMARY as c_int;
    }
    if f.replica() != 0 && f.monitor() == 0 {
        return CLIENT_TYPE_REPLICA as c_int;
    }
    if f.pubsub() != 0 {
        return CLIENT_TYPE_PUBSUB as c_int;
    }
    if !(*c).slot_migration_job.is_null() {
        return if isImportSlotMigrationJob((*c).slot_migration_job) {
            CLIENT_TYPE_SLOT_IMPORT as c_int
        } else {
            CLIENT_TYPE_SLOT_EXPORT as c_int
        };
    }
    CLIENT_TYPE_NORMAL as c_int
}

/* ------------------------------------------------------------------------ */
/* Shared query buffer                                                      */
/* ------------------------------------------------------------------------ */

/// initSharedQueryBuf() networking.c:3924
/// Need to ensure the initlen is not less than readlen in readToQueryBuf.
#[no_mangle]
pub unsafe extern "C" fn initSharedQueryBuf() {
    let querybuf = sdsnewlen(null(), PROTO_IOBUF_LEN as usize);
    sdsclear(querybuf);
    // Stores both the _Thread_local thread_shared_qb and the fork registry.
    rustReadSetThreadSharedQb(querybuf);
}

/// freeSharedQueryBuf() networking.c:3931
#[no_mangle]
pub unsafe extern "C" fn freeSharedQueryBuf() {
    sdsfree(thread_shared_qb());
    rustReadSetThreadSharedQb(null_mut());
}

/// resetSharedQueryBuf() networking.c:2588
/// If any data remained in the buffer, the client takes ownership of the
/// buffer and a new empty buffer is allocated for the shared buffer.
#[no_mangle]
pub unsafe extern "C" fn resetSharedQueryBuf(c: *mut client) {
    server_assert!(c, (*c).querybuf == thread_shared_qb());
    let remaining = sdslen((*c).querybuf) - (*c).qb_pos;

    if remaining > 0 {
        /* Let the client take ownership of the shared buffer. */
        initSharedQueryBuf();
        return;
    }

    (*c).querybuf = null_mut();
    sdsclear(thread_shared_qb());
    (*c).qb_pos = 0;
    (*c).qb_applied = 0;
}

/* ------------------------------------------------------------------------ */
/* Read accounting                                                          */
/* ------------------------------------------------------------------------ */

/// handleReadResult() networking.c:3629
/// Called only from the main thread. Returns C_OK if the querybuf can be
/// further processed, C_ERR if not.
#[no_mangle]
pub unsafe extern "C" fn handleReadResult(c: *mut client) -> c_int {
    server_assert!(c, inMainThread() != 0);
    server.stat_total_reads_processed += 1;
    if (*c).nread <= 0 {
        if (*c).nread == -1 {
            if conn_get_state((*c).conn) != ConnectionState_CONN_STATE_CONNECTED as c_int {
                server_log!(LL_VERBOSE, "Reading from client: %s", conn_get_last_error((*c).conn));
                freeClientAsync(c);
            }
        } else if (*c).nread == 0 {
            if server.verbosity <= LL_VERBOSE as c_int {
                let info = catClientInfoString(sdsempty(), c, server.hide_user_data_from_log);
                server_log!(LL_VERBOSE, "Client closed connection %s", info);
                sdsfree(info);
            }
            freeClientAsync(c);
        }
        return C_ERR as c_int;
    }

    (*c).last_interaction = unixtime() as _;
    (*c).net_input_bytes += (*c).nread as u64;
    if isReplicatedClient(c) != 0 {
        /* A reader-active primary link advances read_reploff with decoded
         * bytes (replDecodeToQueryBuf); c->nread counts wire bytes here. */
        if !(flag(c).primary() != 0 && !server.repl_stream_reader.is_null()) {
            (*(*c).repl_data).read_reploff += (*c).nread as i64;
        }
        if get_client_type(c) == CLIENT_TYPE_PRIMARY as c_int {
            server.stat_net_repl_input_bytes += (*c).nread as i64;
        } else {
            server.stat_net_cluster_slot_import_bytes += (*c).nread as i64;
        }
    } else {
        server.stat_net_input_bytes += (*c).nread as i64;
    }

    /* Handle QB limit */
    if (*c).read_flags & READ_FLAGS_QB_LIMIT_REACHED as c_int != 0 {
        handleQbLimitReached(c);
        return C_ERR as c_int;
    }
    C_OK as c_int
}

/* ------------------------------------------------------------------------ */
/* Parsing                                                                  */
/* ------------------------------------------------------------------------ */

/// parseInlineBuffer() networking.c:3978
/// Like parseMultibulkBuffer(), but for the inline protocol instead of RESP.
/// Sets the client read_flags to indicate the parsing outcome.
#[no_mangle]
pub unsafe extern "C" fn parseInlineBuffer(c: *mut client) {
    let mut argc: c_int = 0;
    let mut linefeed_chars: usize = 1;
    let is_replicated = (*c).read_flags & READ_FLAGS_REPLICATED as c_int;
    let qb = (*c).querybuf;
    let start = qb.add((*c).qb_pos);
    let avail = sdslen(qb) - (*c).qb_pos;

    /* Search for end of line */
    let mut newline = memchr(start as *const c_void, b'\n' as c_int, avail) as *mut c_char;

    /* Nothing to do without a \r\n */
    if newline.is_null() {
        if avail > PROTO_INLINE_MAX_SIZE as usize {
            (*c).read_flags |= READ_FLAGS_ERROR_BIG_INLINE_REQUEST as c_int;
        }
        return;
    }

    /* Handle the \r\n case. */
    if newline != start && *newline.offset(-1) == b'\r' as c_char {
        newline = newline.offset(-1);
        linefeed_chars += 1;
    }

    /* Split the input buffer up to the \r\n */
    let querylen = newline.offset_from(start) as usize;

    /* Reject a raw NUL byte in the line, because sdssplitargs() doesn't
     * handle it. Binary payloads must use the quoted \x00 escape form. */
    if !memchr(start as *const c_void, 0, querylen).is_null() {
        (*c).read_flags |= READ_FLAGS_ERROR_NUL_IN_INLINE_PROTOCOL as c_int;
        return;
    }

    let argv = sdsnsplitargs(start, querylen, &mut argc);
    if argv.is_null() {
        (*c).read_flags |= READ_FLAGS_ERROR_UNBALANCED_QUOTES as c_int;
        return;
    }

    if querylen == 0 {
        (*c).read_flags |= READ_FLAGS_INLINE_ZERO_QUERY_LEN as c_int;
    }

    /* Primaries should never send us inline protocol to run actual
     * commands; they may send just a newline to keep the connection active. */
    if querylen != 0 && is_replicated != 0 {
        sdsfreesplitres(argv, argc);
        (*c).read_flags |= READ_FLAGS_ERROR_UNEXPECTED_INLINE_FROM_REPLICATED_CLIENT as c_int;
        return;
    }

    /* Move querybuffer position to the next query in the buffer. */
    (*c).qb_pos += querylen + linefeed_chars;

    /* Set up argv array on client structure */
    if argc != 0 {
        if !(*c).argv.is_null() {
            zfree((*c).argv as *mut c_void);
        }
        (*c).argv_len = argc;
        (*c).argv = zmalloc(core::mem::size_of::<*mut robj>() * (*c).argv_len as usize) as *mut *mut robj;
        (*c).argv_len_sum = 0;
    }

    /* Create an Object for all arguments. */
    (*c).argc = 0;
    for j in 0..argc as usize {
        /* Strings returned from sdssplitargs() may have unused capacity that we can trim. */
        let a = sdsRemoveFreeSpace(*argv.add(j), 1);
        *argv.add(j) = a;
        *(*c).argv.add((*c).argc as usize) = createObject(OBJ_STRING as c_int, a as *mut c_void);
        (*c).argc += 1;
        (*c).argv_len_sum += sdslen(a);
    }
    zfree(argv as *mut c_void);

    /* Per-slot network bytes-in: every whitespace is of length 1, with the
     * exception of the trailing '\r\n' being length 2. Same unsigned
     * wraparound as C when argc == 0. */
    (*c).net_input_bytes_curr_cmd =
        ((*c).argv_len_sum as u64).wrapping_add(((*c).argc - 1) as i64 as u64).wrapping_add(2);
    (*c).read_flags |= READ_FLAGS_PARSING_COMPLETED as c_int;
    /* Record qb_pos so commandProcessed() can update reploff precisely. */
    (*c).qb_applied = (*c).qb_pos;
    (*c).reqtype = 0;
}

/// parseMultibulkBuffer() networking.c:4118
/// Parses the current command into the client and any further pipelined
/// commands into c->cmd_queue.
#[no_mangle]
pub unsafe extern "C" fn parseMultibulkBuffer(c: *mut client) {
    let mut flag = parse_multibulk(
        c,
        ptr::addr_of_mut!((*c).argc),
        ptr::addr_of_mut!((*c).argv),
        ptr::addr_of_mut!((*c).argv_len),
        ptr::addr_of_mut!((*c).argv_len_sum),
        ptr::addr_of_mut!((*c).net_input_bytes_curr_cmd),
    );
    (*c).read_flags |= flag;

    /* Record qb_pos for commandProcessed(). Written unconditionally because a
     * client may become replicated mid-command. */
    if (*c).read_flags & READ_FLAGS_PARSING_COMPLETED as c_int != 0 {
        (*c).qb_applied = (*c).qb_pos;
    }

    if (*c).read_flags & READ_FLAGS_AUTH_REQUIRED as c_int != 0 {
        /* Execute client's AUTH command before parsing more, because it affects
         * parser limits for max allowed bulk and multibulk lengths. */
        return;
    }

    /* Try parsing pipelined commands. */
    let queue = ptr::addr_of_mut!((*c).cmd_queue);
    server_assert!(c, (*queue).len == 0);
    while (flag & READ_FLAGS_PARSING_COMPLETED as c_int) != 0
        && sdslen((*c).querybuf) > (*c).qb_pos
        && *(*c).querybuf.add((*c).qb_pos) == b'*' as c_char
    {
        (*c).reqtype = PROTO_REQ_MULTIBULK as c_int;
        /* Push a new parser state to the command queue */
        if (*queue).len == (*queue).cap {
            if (*queue).cap == 0 {
                (*queue).cap = COMMAND_QUEUE_MIN_CAPACITY;
            } else if (*queue).cap <= 512 {
                (*queue).cap *= 2;
            } else {
                break; /* Limit the length of the command queue. */
            }
            (*queue).cmds = zrealloc(
                (*queue).cmds as *mut c_void,
                (*queue).cap as usize * core::mem::size_of::<parsedCommand>(),
            ) as *mut parsedCommand;
        }
        let p = (*queue).cmds.add((*queue).len as usize);
        (*queue).len += 1;
        memset(p as *mut c_void, 0, core::mem::size_of::<parsedCommand>());
        flag = parse_multibulk(
            c,
            ptr::addr_of_mut!((*p).argc),
            ptr::addr_of_mut!((*p).argv),
            ptr::addr_of_mut!((*p).argv_len),
            ptr::addr_of_mut!((*p).argv_len_sum),
            ptr::addr_of_mut!((*p).input_bytes),
        );
        (*p).read_flags = flag;
        (*p).slot = -1;
    }
}

/// parseMultibulk() networking.c:4175
/// Incremental parsing of one command in the client's query buffer. Buffer
/// state lives in the client (querybuf, qb_pos, multibulklen, bulklen);
/// command state is passed by pointer. Returns 0 if more data is needed,
/// otherwise READ_FLAGS_PARSING_COMPLETED or a READ_FLAGS_ERROR_* flag.
unsafe fn parse_multibulk(
    c: *mut client,
    argc: *mut c_int,
    argv: *mut *mut *mut robj,
    argv_len: *mut c_int,
    argv_len_sum: *mut usize,
    net_input_bytes_curr_cmd: *mut u64,
) -> c_int {
    let mut ll: i64 = 0;
    let is_replicated = (*c).read_flags & READ_FLAGS_REPLICATED as c_int;
    let auth_required = (*c).read_flags & READ_FLAGS_AUTH_REQUIRED as c_int;

    if (*c).multibulklen == 0 {
        /* The client (argc) should have been reset */
        server_assert!(c, *argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        let start = (*c).querybuf.add((*c).qb_pos);
        let avail = sdslen((*c).querybuf) - (*c).qb_pos;
        let newline = memchr(start as *const c_void, b'\r' as c_int, avail) as *mut c_char;
        if newline.is_null() {
            if avail > PROTO_INLINE_MAX_SIZE as usize {
                return READ_FLAGS_ERROR_BIG_MULTIBULK as c_int;
            }
            return 0;
        }

        /* Buffer should also contain \n */
        if newline.offset_from(start) > avail as isize - 2 {
            return 0;
        }

        /* Check that what follows \r is a real \n */
        if *newline.add(1) != b'\n' as c_char {
            return READ_FLAGS_ERROR_INVALID_CRLF as c_int;
        }

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        server_assert!(c, *start == b'*' as c_char);
        let multibulklen_slen = newline.offset_from(start.add(1)) as usize;
        let ok = string2ll(start.add(1), multibulklen_slen, &mut ll);
        if ok == 0 || ll > INT_MAX as i64 {
            return READ_FLAGS_ERROR_INVALID_MULTIBULK_LEN as c_int;
        } else if ll > 10 && auth_required != 0 {
            return READ_FLAGS_ERROR_UNAUTHENTICATED_MULTIBULK_LEN as c_int;
        }

        (*c).qb_pos = newline.offset_from((*c).querybuf) as usize + 2;

        if ll <= 0 {
            return READ_FLAGS_PARSING_NEGATIVE_MBULK_LEN as c_int;
        }

        (*c).multibulklen = ll as c_int;
        (*c).bulklen = -1;

        /* Set up argv array */
        if !(*argv).is_null() {
            zfree(*argv as *mut c_void);
        }
        *argv_len = (*c).multibulklen.min(1024);
        *argv = zmalloc(core::mem::size_of::<*mut robj>() * *argv_len as usize) as *mut *mut robj;
        *argv_len_sum = 0;

        /* Per-slot network bytes-in, 1st component: "*<n>\r\n". */
        *net_input_bytes_curr_cmd += (multibulklen_slen + 3) as u64;
    }

    server_assert!(c, (*c).multibulklen > 0);
    while (*c).multibulklen != 0 {
        /* Read bulk length if unknown */
        if (*c).bulklen == -1 {
            let start = (*c).querybuf.add((*c).qb_pos);
            let avail = sdslen((*c).querybuf) - (*c).qb_pos;
            let newline = memchr(start as *const c_void, b'\r' as c_int, avail) as *mut c_char;
            if newline.is_null() {
                if avail > PROTO_INLINE_MAX_SIZE as usize {
                    return READ_FLAGS_ERROR_BIG_BULK_COUNT as c_int;
                }
                break;
            }

            /* Buffer should also contain \n */
            if newline.offset_from(start) > avail as isize - 2 {
                return 0;
            }

            if *start != b'$' as c_char {
                return READ_FLAGS_ERROR_MBULK_UNEXPECTED_CHARACTER as c_int;
            }

            /* Check that what follows \r is a real \n */
            if *newline.add(1) != b'\n' as c_char {
                return READ_FLAGS_ERROR_INVALID_CRLF as c_int;
            }

            let bulklen_slen = newline.offset_from(start.add(1)) as usize;
            let ok = string2ll(start.add(1), bulklen_slen, &mut ll);
            if ok == 0 || ll < 0 || (is_replicated == 0 && ll > server.proto_max_bulk_len) {
                return READ_FLAGS_ERROR_MBULK_INVALID_BULK_LEN as c_int;
            } else if ll > 16384 && auth_required != 0 {
                return READ_FLAGS_ERROR_UNAUTHENTICATED_BULK_LEN as c_int;
            }

            (*c).qb_pos = newline.offset_from((*c).querybuf) as usize + 2;
            if is_replicated == 0 && ll >= PROTO_MBULK_BIG_ARG as i64 {
                /* Big-argument optimization: if we are going to read a large
                 * object, try to make it start at c->querybuf boundary so
                 * object creation can take the sds without a copy. Only when
                 * the unparsed data is at most ll+2; otherwise the querybuf
                 * holds more than our bulk and trimming is a waste. Replicated
                 * clients are excluded because their querybuf can only be
                 * trimmed after data is applied and proxied. */
                if sdslen((*c).querybuf) - (*c).qb_pos <= ll as usize + 2 {
                    if (*c).querybuf == thread_shared_qb() {
                        /* Let the client take the ownership of the shared buffer. */
                        initSharedQueryBuf();
                    }
                    sdsrange((*c).querybuf, (*c).qb_pos as isize, -1);
                    (*c).qb_pos = 0;
                    (*c).qb_applied = 0;
                    /* Hint the sds library about the amount of bytes this string is
                     * going to contain. */
                    (*c).querybuf =
                        sdsMakeRoomForNonGreedy((*c).querybuf, ll as usize + 2 - sdslen((*c).querybuf));
                    /* Over-allocated on purpose; don't let it be shrunk before used. */
                    if (*c).querybuf_peak < ll as usize + 2 {
                        (*c).querybuf_peak = ll as usize + 2;
                    }
                }
            }
            (*c).bulklen = ll as _;
            /* Per-slot network bytes-in, 2nd component: "$<n>\r\n". */
            *net_input_bytes_curr_cmd += (bulklen_slen + 3) as u64;
        }

        /* Read bulk argument */
        if sdslen((*c).querybuf) - (*c).qb_pos < ((*c).bulklen + 2) as usize {
            /* Not enough data (+2 == trailing \r\n) */
            break;
        } else {
            /* Check if we have space in argv, grow if needed */
            if *argc >= *argv_len {
                *argv_len = (if *argv_len < INT_MAX / 2 { *argv_len * 2 } else { INT_MAX })
                    .min(*argc + (*c).multibulklen);
                *argv = zrealloc(
                    *argv as *mut c_void,
                    core::mem::size_of::<*mut robj>() * *argv_len as usize,
                ) as *mut *mut robj;
            }

            /* Check that what follows argv is a real \r\n */
            let end = (*c).querybuf.add((*c).qb_pos + (*c).bulklen as usize);
            if *end != b'\r' as c_char || *end.add(1) != b'\n' as c_char {
                return READ_FLAGS_ERROR_INVALID_CRLF as c_int;
            }

            /* Optimization: if a non-replicated client's buffer contains JUST
             * our bulk element, use the sds itself instead of copying it. */
            if is_replicated == 0
                && (*c).qb_pos == 0
                && (*c).bulklen >= PROTO_MBULK_BIG_ARG as _
                && sdslen((*c).querybuf) == ((*c).bulklen + 2) as usize
            {
                *(*argv).add(*argc as usize) = createObject(OBJ_STRING as c_int, (*c).querybuf as *mut c_void);
                *argc += 1;
                *argv_len_sum += (*c).bulklen as usize;
                sdsIncrLen((*c).querybuf, -2); /* remove CRLF */
                /* Assume that if we saw a fat argument we'll see another one
                 * likely... */
                (*c).querybuf = sdsnewlen(SDS_NOINIT as *const c_void, ((*c).bulklen + 2) as usize);
                sdsclear((*c).querybuf);
            } else {
                *(*argv).add(*argc as usize) =
                    createStringObject((*c).querybuf.add((*c).qb_pos), (*c).bulklen as usize);
                *argc += 1;
                *argv_len_sum += (*c).bulklen as usize;
                (*c).qb_pos += ((*c).bulklen + 2) as usize;
            }
            (*c).bulklen = -1;
            (*c).multibulklen -= 1;
        }
    }

    /* We're done when c->multibulklen == 0 */
    if (*c).multibulklen == 0 {
        /* Per-slot network bytes-in, 3rd and 4th components. */
        *net_input_bytes_curr_cmd += (*argv_len_sum + (*argc as usize * 2)) as u64;
        (*c).reqtype = 0;
        return READ_FLAGS_PARSING_COMPLETED as c_int;
    }
    0
}

/// parseInputBuffer() networking.c:4519
/// Parse one or more commands from the query buf. Called from the main
/// thread or an IO thread.
#[no_mangle]
pub unsafe extern "C" fn parseInputBuffer(c: *mut client) {
    /* The command queue must be emptied before parsing. */
    server_assert!(c, (*c).cmd_queue.len == 0);

    /* Hold off transport read handlers for the duration of parsing (RDMA update_state
     * may otherwise re-enter readQueryFromClient and corrupt cmd_queue). */
    let conn = (*c).conn;
    if !conn.is_null() {
        conn_set_postpone_update_state(
            conn,
            client_conn_postpone_mask_from_io_state(c) | CONN_POSTPONE_READ as c_int,
        );
    }

    /* Determine request type when unknown. */
    if (*c).reqtype == 0 {
        if *(*c).querybuf.add((*c).qb_pos) == b'*' as c_char {
            (*c).reqtype = PROTO_REQ_MULTIBULK as c_int;
        } else {
            (*c).reqtype = PROTO_REQ_INLINE as c_int;
        }
    }

    if (*c).reqtype == PROTO_REQ_INLINE as c_int {
        parseInlineBuffer(c);
    } else if (*c).reqtype == PROTO_REQ_MULTIBULK as c_int {
        parseMultibulkBuffer(c);
    } else {
        _serverPanic(
            concat!(file!(), "\0").as_ptr() as *const c_char,
            line!() as c_int,
            c"Unknown request type".as_ptr(),
        );
        std::process::abort();
    }

    /* Restore only IO-state postpone; do not hold READ for cmd_queue here. */
    if !conn.is_null() {
        conn_set_postpone_update_state(conn, client_conn_postpone_mask_from_io_state(c));
    }
}

/* ------------------------------------------------------------------------ */
/* Reading                                                                  */
/* ------------------------------------------------------------------------ */

/// readToQueryBuf() networking.c:4802
/// Allocates the query buffer if required and reads into it from the
/// network through connRead(). Sets c->nread. Called from the main thread or
/// an IO thread. Returns true if the buffer was filled (more may be available).
#[no_mangle]
pub unsafe extern "C" fn readToQueryBuf(c: *mut client) -> bool {
    let mut big_arg = false;

    /* If the replica RDB client is marked as closed ASAP, do not try to read from it */
    if flag(c).close_asap() != 0 {
        (*c).nread = 0;
        return false;
    }

    let is_replicated = (*c).read_flags & READ_FLAGS_REPLICATED as c_int;

    let mut readlen: usize = PROTO_IOBUF_LEN as usize;
    let mut qblen: usize = if (*c).querybuf.is_null() { 0 } else { sdslen((*c).querybuf) };
    /* If this is a multi bulk request, and we are processing a bulk reply
     * that is large enough, try to maximize the probability that the query
     * buffer contains exactly the SDS string representing the object, even
     * at the risk of requiring more read(2) calls. */
    if (*c).reqtype == PROTO_REQ_MULTIBULK as c_int
        && (*c).multibulklen != 0
        && (*c).bulklen != -1
        && (*c).bulklen >= PROTO_MBULK_BIG_ARG as _
    {
        let remaining: isize = ((*c).bulklen as usize + 2).wrapping_sub(qblen - (*c).qb_pos) as isize;
        big_arg = true;

        /* Note that the 'remaining' variable may be zero in some edge case,
         * for example once we resume a blocked client after CLIENT PAUSE. */
        if remaining > 0 {
            readlen = remaining as usize;
        }

        /* Replicated client needs expand the readlen when meet BIG_ARG(see #9100),
         * but doesn't need align to the next arg, we can read more data. */
        if isReplicatedClient(c) != 0 && readlen < PROTO_IOBUF_LEN as usize {
            readlen = PROTO_IOBUF_LEN as usize;
        }
    }

    if (*c).querybuf.is_null() {
        server_assert!(c, sdslen(thread_shared_qb()) == 0);
        (*c).querybuf = if big_arg { sdsempty() } else { thread_shared_qb() };
        qblen = sdslen((*c).querybuf);
    }

    /* c->querybuf may be expanded. If so, the old thread_shared_qb will be
     * released. We ensure it is not expanded while shared, but check anyway. */
    let use_thread_shared_qb = (*c).querybuf == thread_shared_qb();
    if is_replicated == 0 && (big_arg || sdsalloc((*c).querybuf) < PROTO_IOBUF_LEN as usize) {
        /* Non-greedy growth for BIG_ARG reads and for an initial allocation,
         * to avoid collision with the RESIZE_THRESHOLD mechanism. */
        (*c).querybuf = sdsMakeRoomForNonGreedy((*c).querybuf, readlen);
        /* Over-allocated on purpose; don't let it be shrunk before used. */
        if (*c).querybuf_peak < qblen + readlen {
            (*c).querybuf_peak = qblen + readlen;
        }
    } else {
        (*c).querybuf = sdsMakeRoomFor((*c).querybuf, readlen);

        /* Read as much as possible from the socket to save read(2) system calls. */
        readlen = sdsavail((*c).querybuf);
    }
    if use_thread_shared_qb {
        server_assert!(c, (*c).querybuf == thread_shared_qb());
    }

    (*c).nread = conn_read((*c).conn, (*c).querybuf.add(qblen) as *mut c_void, readlen);
    if (*c).nread <= 0 {
        return false;
    }

    sdsIncrLen((*c).querybuf, (*c).nread as isize);
    qblen = sdslen((*c).querybuf);
    if (*c).querybuf_peak < qblen {
        (*c).querybuf_peak = qblen;
    }
    if is_replicated == 0 {
        /* MULTI/EXEC queued arguments count as query buffer too. For
         * unauthenticated clients, the query buffer cannot exceed 1MB. */
        let qb_memory = sdslen((*c).querybuf)
            + if (*c).mstate.is_null() { 0 } else { (*(*c).mstate).argv_len_sums };
        if qb_memory > server.client_max_querybuf_len
            || (qb_memory > 1024 * 1024 && ((*c).read_flags & READ_FLAGS_AUTH_REQUIRED as c_int) != 0)
        {
            (*c).read_flags |= READ_FLAGS_QB_LIMIT_REACHED as c_int;
        }
    }
    (*c).nread as usize == readlen
}

/// readAndDecodePrimaryStream() networking.c:4889
/// Keep the wire scratch buffer off the stack of ordinary client reads.
#[inline(never)]
unsafe fn read_and_decode_primary_stream(
    primary: *mut client,
    output_budget: usize,
    decoded_bytes: &mut isize,
    full_read: &mut bool,
) -> bool {
    let mut wire_buf = core::mem::MaybeUninit::<[u8; PROTO_IOBUF_LEN as usize]>::uninit();
    if flag(primary).close_asap() != 0 {
        (*primary).nread = 0;
        *full_read = false;
    } else {
        (*primary).nread = conn_read((*primary).conn, wire_buf.as_mut_ptr() as *mut c_void, PROTO_IOBUF_LEN as usize);
        *full_read = (*primary).nread == PROTO_IOBUF_LEN as c_int;
    }
    if handleReadResult(primary) != C_OK as c_int {
        return false;
    }

    *decoded_bytes = replDecodeToQueryBuf(
        primary,
        wire_buf.as_ptr() as *const c_void,
        (*primary).nread as usize,
        output_budget,
    );
    true
}

/// readQueryFromClient() networking.c:4908
/// The connection read handler.
#[no_mangle]
pub unsafe extern "C" fn readQueryFromClient(conn: *mut connection) {
    let c = (*conn).private_data as *mut client;
    /* Check if we can send the client to be handled by the IO-thread */
    if postponeClientRead(c) != 0 {
        return;
    }

    if io_write_state(c) != CLIENT_IDLE || io_read_state(c) != CLIENT_IDLE {
        return;
    }

    let mut iter: c_int = 0;
    let mut decoded_bytes: usize = 0;
    loop {
        let mut decoded_this_iteration: isize = 0;
        let mut full_read: bool;
        let mut read_ok = true;
        let decode_repl_stream = flag(c).primary() != 0 && !server.repl_stream_reader.is_null();
        let resume_decode = decode_repl_stream && replStreamHasPendingDecode();
        let decode_budget = REPL_DECODE_EVENT_BUDGET as usize - decoded_bytes;
        if resume_decode {
            decoded_this_iteration = replDecodeToQueryBuf(c, null(), 0, decode_budget);
            full_read = !replStreamHasPendingDecode();
        } else if decode_repl_stream {
            full_read = false;
            read_ok = read_and_decode_primary_stream(c, decode_budget, &mut decoded_this_iteration, &mut full_read);
        } else {
            full_read = readToQueryBuf(c);
            read_ok = handleReadResult(c) == C_OK as c_int;
        }
        if read_ok {
            if decoded_this_iteration < 0 {
                server_log!(LL_WARNING, "Disconnecting primary due to replication stream decompression failure");
                freeClientAsync(c);
                return;
            }
            decoded_bytes += decoded_this_iteration as usize;
            if processInputBuffer(c) == C_ERR as c_int {
                return;
            }
            trimCommandQueue(c);
            if decode_repl_stream && replStreamHasPendingDecode() {
                full_read = false;
            }
        }
        iter += 1;
        let repeat = flag(c).primary() != 0
            && flag(c).close_asap() == 0
            && iter < REPL_MAX_READS_PER_IO_EVENT
            && decoded_bytes < REPL_DECODE_EVENT_BUDGET as usize
            && full_read;
        beforeNextClient(c);
        if !repeat {
            break;
        }
    }
}
