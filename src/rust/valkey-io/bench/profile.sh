#!/bin/bash
# perf record (DWARF call graphs, cpu-clock: this KVM guest has no PMU) of one
# server thread at a saturation point.
#   profile.sh <server-bin> <config> <thread: main|io0> <test> <pipe> <outdir>
# config as in e2e.py (c-io2, io-t1, precheck-t1, ...). Writes <outdir>/<name>.{data,txt,folded,svg,groups.txt}.
set -e
BIN=$1; CFG=$2; THR=$3; TEST=$4; PIPE=$5; OUT=$6
HERE=$(cd "$(dirname "$0")" && pwd)
BENCH=${BENCH:-$(dirname "$BIN")/valkey-benchmark}
CLI=${CLI:-$(dirname "$BIN")/valkey-cli}
FG=${FG:-$HOME/git/FlameGraph}
PORT=${PORT:-7931}; SECS=${SECS:-6}; FREQ=${FREQ:-999}
# DATASIZE/KEYS/CLIENTS: value size, keyspace and clients; EXTRA: more server args; TAG: name suffix.
D=${DATASIZE:-3}; K=${KEYS:-1000000}; C=${CLIENTS:-200}
mkdir -p "$OUT"; NAME=$CFG-$THR-$TEST-P$PIPE${TAG:+-$TAG}
case $CFG in
  c-io*) ARGS="--io-threads ${CFG#c-io}";;
  io-t*) ARGS="--rust-io-takeover yes --rust-io-mode io --rust-io-threads ${CFG#io-t}";;
  precheck-t*) ARGS="--rust-io-takeover yes --rust-io-mode precheck --rust-io-threads ${CFG#precheck-t}";;
  rtc-t*) ARGS="--rust-io-takeover yes --rust-io-mode exec --rust-io-exec-model rtc --rust-io-threads ${CFG#rtc-t}";;
esac
taskset -c 0-3 "$BIN" --port $PORT --save "" --appendonly no --dir /tmp --logfile /tmp/rio-prof-$PORT.log $ARGS $EXTRA &
SPID=$!
trap 'taskset -c 4-7 "$CLI" -p $PORT shutdown nosave >/dev/null 2>&1 || kill $SPID; wait $SPID 2>/dev/null' EXIT
for i in $(seq 50); do [ "$(taskset -c 4-7 "$CLI" -p $PORT ping 2>/dev/null)" = PONG ] && break; sleep 0.1; done
if [ "$THR" = main ]; then TID=$SPID; else
  TID=$(for t in /proc/$SPID/task/*; do n=$(cat $t/comm); case $n in rust-io-${THR#io}|io_thd_${THR#io}) basename $t;; esac; done | head -1)
fi
[ -n "$TID" ] || { echo "no thread $THR"; exit 1; }
# Warm up (fills keys for GET), then record mid-run.
taskset -c 4-7 "$BENCH" -p $PORT -t set -P 16 -c $C -n $((K < 1000000 ? 3 * K : 3000000)) -d $D -r $K --threads 4 -q >/dev/null
taskset -c 4-7 "$BENCH" -p $PORT -t $TEST -P $PIPE -c $C -n 100000000 -d $D -r $K --threads 4 -q >/dev/null &
BPID=$!
sleep 1.5
sudo perf record -F $FREQ -e cpu-clock --call-graph dwarf,16384 -t $TID -o "$OUT/$NAME.data" -- sleep $SECS 2> "$OUT/$NAME.record.log"
kill $BPID; wait $BPID 2>/dev/null || true
sudo chown $(id -u) "$OUT/$NAME.data"
"$HERE/profpost.sh" "$OUT/$NAME.data"
echo "$OUT/$NAME"
