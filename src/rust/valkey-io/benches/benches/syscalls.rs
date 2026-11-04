//! Syscall costs on this host, the floor under every handoff.
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;

fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("syscall");
    let efd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
    g.bench_function("eventfd_write_read", |b| {
        b.iter(|| {
            let one: u64 = 1;
            let mut v: u64 = 0;
            unsafe {
                libc::write(efd, &one as *const u64 as *const libc::c_void, 8);
                libc::read(efd, &mut v as *mut u64 as *mut libc::c_void, 8);
            }
            black_box(v)
        })
    });
    let ep = unsafe { libc::epoll_create1(0) };
    let mut ev = libc::epoll_event { events: libc::EPOLLIN as u32, u64: 0 };
    unsafe { libc::epoll_ctl(ep, libc::EPOLL_CTL_ADD, efd, &mut ev) };
    let mut evs = [libc::epoll_event { events: 0, u64: 0 }; 64];
    g.bench_function("epoll_wait_0_timeout_idle", |b| {
        b.iter(|| black_box(unsafe { libc::epoll_wait(ep, evs.as_mut_ptr(), 64, 0) }))
    });
    let mut sv = [0i32; 2];
    unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, sv.as_mut_ptr()) };
    let buf = [b'x'; 64];
    let mut rb = [0u8; 64];
    g.bench_function("socketpair_write_64B", |b| {
        b.iter(|| unsafe {
            libc::write(sv[0], buf.as_ptr() as *const libc::c_void, 64);
            // keep the socket from filling; this read is not timed separately
            libc::read(sv[1], rb.as_mut_ptr() as *mut libc::c_void, 64);
        })
    });
    g.bench_function("socketpair_write_read_64B", |b| {
        b.iter(|| unsafe {
            libc::write(sv[0], buf.as_ptr() as *const libc::c_void, 64);
            black_box(libc::read(sv[1], rb.as_mut_ptr() as *mut libc::c_void, 64));
        })
    });
    // TCP loopback, which is what the server actually uses.
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let a = std::net::TcpStream::connect(l.local_addr().unwrap()).unwrap();
    let (s, _) = l.accept().unwrap();
    a.set_nodelay(true).unwrap();
    use std::os::unix::io::AsRawFd;
    let (fa, fs) = (a.as_raw_fd(), s.as_raw_fd());
    g.bench_function("tcp_loopback_write_read_64B", |b| {
        b.iter(|| unsafe {
            libc::write(fa, buf.as_ptr() as *const libc::c_void, 64);
            let mut got = 0;
            while got < 64 {
                got += libc::read(fs, rb.as_mut_ptr().add(got) as *mut libc::c_void, 64 - got) as usize;
            }
        })
    });
    g.bench_function("getpid_floor", |b| b.iter(|| black_box(unsafe { libc::syscall(libc::SYS_getpid) })));
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
