//! Handoff primitives as lib.rs uses them: the per-connection command queue
//! (Mutex<VecDeque<VioCmd>>), the reply buffer, and the eventfd + epoll wake
//! between an IO thread and the main thread.
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::hint::black_box;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use valkey_io::benchapi::{empty_cmd, eventfd_drain, eventfd_signal, CmdQueue};

fn efd() -> i32 {
    unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) }
}

struct Ep(i32);
impl Ep {
    fn new(fd: i32) -> Ep {
        let ep = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        let mut ev = libc::epoll_event { events: libc::EPOLLIN as u32, u64: 0 };
        unsafe { libc::epoll_ctl(ep, libc::EPOLL_CTL_ADD, fd, &mut ev) };
        Ep(ep)
    }
    fn wait(&self) {
        let mut evs = [libc::epoll_event { events: 0, u64: 0 }; 4];
        loop {
            let n = unsafe { libc::epoll_wait(self.0, evs.as_mut_ptr(), 4, -1) };
            if n > 0 {
                return;
            }
        }
    }
}

fn queue(c: &mut Criterion) {
    let mut g = c.benchmark_group("queue");
    let q = CmdQueue::default();
    let mut out: Vec<MaybeUninit<_>> = (0..16).map(|_| MaybeUninit::uninit()).collect();
    g.throughput(Throughput::Elements(1));
    g.bench_function("push_pop_1", |b| {
        let mut batch = Vec::with_capacity(16);
        b.iter(|| {
            batch.push(empty_cmd());
            q.push_batch(&mut batch);
            black_box(q.pop(&mut out[..1]));
        })
    });
    g.throughput(Throughput::Elements(16));
    g.bench_function("push_pop_batch16", |b| {
        let mut batch = Vec::with_capacity(16);
        b.iter(|| {
            for _ in 0..16 {
                batch.push(empty_cmd());
            }
            q.push_batch(&mut batch);
            black_box(q.pop(&mut out[..16]));
        })
    });
    let reply = [b'+'; 5];
    let mut local = Vec::new();
    g.throughput(Throughput::Elements(1));
    g.bench_function("reply_append_swap_5B", |b| {
        b.iter(|| {
            q.append_out(black_box(&reply));
            q.swap_out(&mut local);
        })
    });
    g.finish();
}

/// One request/response through the real wake path: the "IO thread" pushes
/// a command and signals the main eventfd; the "main thread" (epoll_wait,
/// drain, pop) appends a reply and signals the IO eventfd; the IO thread
/// wakes in epoll_wait and swaps the reply out. `noise` extra threads hammer
/// the same queue mutex to model several IO threads contending.
fn round_trip(c: &mut Criterion, name: &str, noise: usize) {
    let q = Arc::new(CmdQueue::default());
    let (to_main, to_io) = (efd(), efd());
    let stop = Arc::new(AtomicBool::new(false));
    let main = {
        let (q, stop) = (q.clone(), stop.clone());
        std::thread::spawn(move || {
            let ep = Ep::new(to_main);
            let mut out: Vec<MaybeUninit<_>> = (0..16).map(|_| MaybeUninit::uninit()).collect();
            while !stop.load(Ordering::Relaxed) {
                ep.wait();
                eventfd_drain(to_main);
                if q.pop(&mut out) > 0 {
                    q.append_out(b"+OK\r\n");
                    eventfd_signal(to_io);
                }
            }
        })
    };
    let noisy: Vec<_> = (0..noise)
        .map(|_| {
            let (q, stop) = (q.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut local = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    q.append_out(b"");
                    q.swap_out(&mut local);
                }
            })
        })
        .collect();
    let ep = Ep::new(to_io);
    let mut batch = Vec::with_capacity(1);
    let mut local = Vec::new();
    c.bench_function(name, |b| {
        b.iter(|| {
            batch.push(empty_cmd());
            q.push_batch(&mut batch);
            eventfd_signal(to_main);
            ep.wait();
            eventfd_drain(to_io);
            q.swap_out(&mut local);
        })
    });
    stop.store(true, Ordering::Relaxed);
    batch.push(empty_cmd());
    q.push_batch(&mut batch);
    eventfd_signal(to_main);
    main.join().unwrap();
    for t in noisy {
        t.join().unwrap();
    }
}

fn handoff(c: &mut Criterion) {
    round_trip(c, "handoff_round_trip_uncontended", 0);
    round_trip(c, "handoff_round_trip_contended_2", 2);
}

criterion_group!(benches, queue, handoff);
criterion_main!(benches);
