//! RESP parser: ns per command (per frame for the split case).
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::hint::black_box;
use valkey_io::resp::{Limits, Outcome, Parser};

fn set_cmd(val: usize) -> Vec<u8> {
    let v = vec![b'x'; val];
    let mut b = format!("*3\r\n$3\r\nSET\r\n$16\r\nkey:000000123456\r\n${}\r\n", val).into_bytes();
    b.extend_from_slice(&v);
    b.extend_from_slice(b"\r\n");
    b
}

const LIM: Limits = Limits { max_bulk_len: 512 * 1024 * 1024, auth_required: false };

/// Parse every command in `buf`, as Worker::parse_buffered() does. Returns
/// the number of commands.
fn parse_all(p: &mut Parser, buf: &[u8]) -> usize {
    let mut pos = 0;
    let mut n = 0;
    loop {
        match p.parse(&buf[pos..], &LIM) {
            Outcome::Command { args, consumed, .. } => {
                black_box(&args);
                pos += consumed;
                n += 1;
            }
            Outcome::Empty { consumed } => pos += consumed,
            Outcome::Incomplete => return n,
            Outcome::Error(..) => panic!("parse error"),
        }
    }
}

fn bench(c: &mut Criterion) {
    let get = b"*2\r\n$3\r\nGET\r\n$16\r\nkey:000000123456\r\n".to_vec();
    let mut g = c.benchmark_group("resp");
    let mut one = |name: &str, buf: Vec<u8>, cmds: u64| {
        g.throughput(Throughput::Elements(cmds));
        g.bench_function(name, |b| {
            let mut p = Parser::new();
            b.iter(|| assert_eq!(parse_all(&mut p, black_box(&buf)), cmds as usize))
        });
    };
    one("get", get.clone(), 1);
    one("set_64B", set_cmd(64), 1);
    one("set_1KB", set_cmd(1024), 1);
    one("set_16KB", set_cmd(16 * 1024), 1);
    one("pipeline16_get", get.repeat(16), 16);
    one("inline_get", b"GET key:000000123456\r\n".to_vec(), 1);
    one("inline_set_64B", format!("SET key:000000123456 {}\r\n", "x".repeat(64)).into_bytes(), 1);
    // A SET 1KB split across two reads in the middle of the value: the first
    // parse returns Incomplete, the second (whole frame now buffered,
    // as the IO thread re-presents rbuf[rpos..]) returns the command.
    let s = set_cmd(1024);
    let cut = s.len() / 2;
    g.throughput(Throughput::Elements(1));
    g.bench_function("set_1KB_split_2_reads", |b| {
        b.iter_batched_ref(
            Parser::new,
            |p| {
                assert!(matches!(p.parse(black_box(&s[..cut]), &LIM), Outcome::Incomplete));
                assert_eq!(parse_all(p, black_box(&s)), 1);
            },
            BatchSize::SmallInput,
        )
    });
    let get_cut = get.len() - 3;
    g.bench_function("get_split_2_reads", |b| {
        b.iter_batched_ref(
            Parser::new,
            |p| {
                assert!(matches!(p.parse(black_box(&get[..get_cut]), &LIM), Outcome::Incomplete));
                assert_eq!(parse_all(p, black_box(&get)), 1);
            },
            BatchSize::SmallInput,
        )
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
