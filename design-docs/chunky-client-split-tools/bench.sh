#!/bin/bash
# Usage: bench.sh OUTDIR ; runs base,s1,s2c,s2 interleaved, 3 rounds.
OUT=$1; mkdir -p $OUT
BIN=/tmp/chunky-client-split-bin
CLI=$BIN/base/valkey-cli; BM=$BIN/base/valkey-benchmark
PORT=46101
for round in 1 2 3; do
  for v in base s1 s2c s2; do
    taskset -c 24 $BIN/$v/valkey-server --port $PORT --save '' --appendonly no --daemonize no --logfile $OUT/server-$v-$round.log &
    PID=$!; echo "started $v pid $PID" >> $OUT/pids.txt
    for i in $(seq 1 200); do $CLI -p $PORT ping 2>/dev/null | grep -q PONG && break; done
    $BM -p $PORT -t set -n 1000000 -r 100000 -P 16 -c 50 --threads 2 -q > /dev/null 2>&1
    for t in get set; do
      echo "== $v round $round $t: taskset -c 26-31 valkey-benchmark -p $PORT -t $t -n 8000000 -r 100000 -P 16 -c 50 --threads 4 -q" >> $OUT/raw.txt
      taskset -c 26-31 $BM -p $PORT -t $t -n 8000000 -r 100000 -P 16 -c 50 --threads 4 -q 2>&1 | tr '\r' '\n' | grep -E "requests per second" | tail -1 >> $OUT/raw.txt
      echo "== $v round $round ${t}-P1: taskset -c 26-31 valkey-benchmark -p $PORT -t $t -n 2000000 -r 100000 -P 1 -c 50 --threads 4 -q" >> $OUT/raw.txt
      taskset -c 26-31 $BM -p $PORT -t $t -n 2000000 -r 100000 -P 1 -c 50 --threads 4 -q 2>&1 | tr '\r' '\n' | grep -E "requests per second" | tail -1 >> $OUT/raw.txt
    done
    $CLI -p $PORT shutdown nosave >/dev/null 2>&1; wait $PID
  done
done
