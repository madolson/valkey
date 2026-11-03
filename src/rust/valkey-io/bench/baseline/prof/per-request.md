
main thread, -P 16, ns per request (self-time share x thread CPU ns/req)

| group | c-io2 get | c-io2 set | io-t1 get | io-t1 set | precheck-t1 get | precheck-t1 set |
|---|---|---|---|---|---|---|
| **thread CPU ns/req** | **955** | **1062** | **833** | **1030** | **779** | **1014** |
| call() and command procs | 182 (19.1%) | 379 (35.7%) | 171 (20.5%) | 335 (32.5%) | 162 (20.8%) | 352 (34.7%) |
| key prefetch (prefetchCommandQueueKeys) | 113 (11.8%) | 117 (11.0%) | 135 (16.2%) | 131 (12.7%) | 152 (19.5%) | 125 (12.3%) |
| syscall:epoll_wait | 158 (16.5%) | 49 (4.6%) | 6 (0.7%) | 2 (0.2%) | 6 (0.8%) | 6 (0.6%) |
| allocator: jemalloc (zmalloc) | 22 (2.3%) | 104 (9.8%) | 84 (10.1%) | 161 (15.6%) | 87 (11.2%) | 155 (15.3%) |
| reply building | 92 (9.6%) | 44 (4.1%) | 110 (13.2%) | 52 (5.0%) | 111 (14.2%) | 54 (5.3%) |
| clock reads (getMonotonicUs, ustime) | 123 (12.9%) | 74 (7.0%) | 61 (7.3%) | 59 (5.7%) | 62 (8.0%) | 59 (5.8%) |
| event loop | 116 (12.2%) | 73 (6.9%) | 1 (0.1%) | 1 (0.1%) | 2 (0.3%) | 1 (0.1%) |
| handoff: pop/drain/kick | 0 (0.0%) | 0 (0.0%) | 85 (10.2%) | 68 (6.6%) | 26 (3.3%) | 25 (2.5%) |
| processCommand checks | 69 (7.2%) | 80 (7.5%) | 81 (9.7%) | 80 (7.8%) | 51 (6.6%) | 56 (5.5%) |
| freeing argv | 29 (3.0%) | 53 (5.0%) | 38 (4.6%) | 52 (5.0%) | 43 (5.5%) | 58 (5.7%) |
| argv build (create_argv: createStringObject) | 0 (0.0%) | 27 (2.5%) | 0 (0.0%) | 29 (2.8%) | 0 (0.0%) | 40 (3.9%) |
| memcpy/libc | 19 (2.0%) | 14 (1.3%) | 28 (3.4%) | 14 (1.4%) | 24 (3.1%) | 17 (1.7%) |
| prepareCommand and lookup | 28 (2.9%) | 36 (3.4%) | 11 (1.3%) | 23 (2.2%) | 13 (1.7%) | 21 (2.1%) |
| precheck (IO-thread checks; main-thread token validate) | 1 (0.1%) | 1 (0.1%) | 1 (0.1%) | 1 (0.1%) | 16 (2.1%) | 19 (1.9%) |
| reply memcpy into Rust (connRustWritev) | 0 (0.0%) | 0 (0.0%) | 9 (1.1%) | 8 (0.8%) | 8 (1.0%) | 8 (0.8%) |

io0 thread, -P 16, ns per request (self-time share x thread CPU ns/req)

| group | io-t1 get | io-t1 set | precheck-t1 get |
|---|---|---|---|
| **thread CPU ns/req** | **936** | **1010** | **1033** |
| syscall:write | 518 (55.3%) | 533 (52.8%) | 497 (48.1%) |
| RESP parse / IO-thread logic | 104 (11.1%) | 124 (12.3%) | 92 (8.9%) |
| allocator: jemalloc (zmalloc) | 57 (6.1%) | 79 (7.8%) | 72 (7.0%) |
| syscall:read | 72 (7.7%) | 71 (7.0%) | 75 (7.3%) |
| argv build (create_argv: createStringObject) | 37 (4.0%) | 64 (6.3%) | 34 (3.3%) |
| allocator: glibc malloc (Rust Vec/VecDeque/Arc) | 45 (4.8%) | 40 (4.0%) | 55 (5.3%) |
| syscall:other | 41 (4.4%) | 38 (3.8%) | 36 (3.5%) |
| handoff: pop/drain/kick | 33 (3.5%) | 33 (3.3%) | 31 (3.0%) |
| call() and command procs | 0 (0.0%) | 0 (0.0%) | 36 (3.5%) |
| precheck (IO-thread checks; main-thread token validate) | 0 (0.0%) | 0 (0.0%) | 33 (3.2%) |
| syscall:epoll_wait | 21 (2.2%) | 16 (1.6%) | 22 (2.1%) |
| processCommand checks | 0 (0.0%) | 0 (0.0%) | 20 (1.9%) |
| prepareCommand and lookup | 0 (0.0%) | 0 (0.0%) | 17 (1.6%) |
| memcpy/libc | 7 (0.7%) | 11 (1.1%) | 9 (0.9%) |
