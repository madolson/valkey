#!/bin/bash
# bench.sh <label> <kind> <args...>
#   kind=single <io-threads> <server-binary>
#   kind=cluster <N> <io-threads>
#   kind=dlhost <N> <M> [extra host args]
# Servers on $SCPU (default 0-7), valkey-benchmark ($CTHR threads) on $CCPU (default 8-13).
# Runs RUNS x {P1,P16} x {set,get}; raw output under $OUT/<label>/.
set -u
LABEL=$1; KIND=$2; shift 2
BASE=/tmp/chunky-dlmopen-base/src
CLI=$BASE/valkey-cli; BENCH=$BASE/valkey-benchmark
OUT=${OUT:-$HOME/chunky/data/dlmopen/bench}
RUNS=${RUNS:-3}
SCPU=${SCPU:-0-7}; CCPU=${CCPU:-8-13}; CTHR=${CTHR:-6}
D=$OUT/$LABEL; mkdir -p $D
W=/tmp/chunky-dlmopen-bench; rm -rf $W; mkdir -p $W
PIDS=()
waitping() { for i in $(seq 1000); do $CLI "$@" ping 2>/dev/null | grep -q PONG && return 0; done; echo "not ready: $*"; return 1; }
for p in 10100 10200 10201 10202 10203 10300; do ss -ltn | grep -q "127.0.0.1:$p \|\*:$p " && { echo "port $p busy"; exit 1; }; done
COMMON=(--save "" --appendonly no --protected-mode no)
case $KIND in
single)
  IOT=$1; BIN=$2
  taskset -c $SCPU $BIN --port 10300 --io-threads $IOT --dir $W --logfile $W/s.log "${COMMON[@]}" &
  PIDS+=($!); waitping -p 10300; BARGS=(-p 10300); TARGET=10300 ;;
cluster)
  N=$1; IOT=$2; NODES=()
  for i in $(seq 0 $((N-1))); do p=$((10200+i)); mkdir -p $W/n$i
    taskset -c $SCPU $BASE/valkey-server --port $p --io-threads $IOT --cluster-enabled yes --cluster-config-file $W/n$i/nodes.conf --dir $W/n$i --logfile $W/n$i/s.log "${COMMON[@]}" &
    PIDS+=($!); NODES+=(127.0.0.1:$p); done
  for i in $(seq 0 $((N-1))); do waitping -p $((10200+i)); done
  $CLI --cluster create "${NODES[@]}" --cluster-replicas 0 --cluster-yes > $D/cluster-create.txt 2>&1
  for i in $(seq 1000); do $CLI -p 10200 cluster info | grep -q "cluster_state:ok" && break; done
  for i in $(seq 0 $((N-1))); do for j in $(seq 1000); do $CLI -p $((10200+i)) cluster info | grep -q "cluster_state:ok" && break; done; done
  BARGS=(--cluster -p 10200); TARGET=10200 ;;
dlhost)
  N=$1; M=$2; shift 2
  RUN=$W/run; mkdir -p $RUN
  taskset -c $SCPU ${DLHOST_DIR:-/tmp/chunky-dlmopen/src}/valkey-dlhost --so ${DLHOST_DIR:-/tmp/chunky-dlmopen/src}/libvalkey-engine.so --engines $N --io-threads $M --port 10100 --rundir $RUN "$@" > $D/host.log 2>&1 &
  PIDS+=($!); waitping -p 10100; BARGS=(${CLUSTERCLIENT:+--cluster} -p 10100); TARGET=10100 ;;
esac
echo "${PIDS[@]}" >> /tmp/chunky-dlmopen-pids
echo "$LABEL $KIND $* pids=${PIDS[*]} $(date -u +%FT%TZ)" > $D/meta.txt
if [ -n "${PERFSTAT:-}" ]; then
  # Per-request main-thread CPU: perf stat on the engine main threads
  # (dlhost: threads named engineN; processes: the main thread, tid == pid).
  TIDS=""
  for p in "${PIDS[@]}"; do
    if [ $KIND = dlhost ]; then
      for t in /proc/$p/task/*; do grep -q '^engine[0-9]*$' $t/comm && TIDS="$TIDS,${t##*/}"; done
    else TIDS="$TIDS,$p"; fi
  done
  TIDS=${TIDS#,}
  for T in set get; do
    CMD="taskset -c $CCPU $BENCH ${BARGS[*]} -c 400 -d 64 -r 1000000 -n 20000000 -P 16 --threads $CTHR -t $T --csv"
    echo "$CMD" > $D/perfstat-$T.txt
    echo "main threads: $TIDS" >> $D/perfstat-$T.txt
    perf stat -e task-clock,cycles:u,instructions:u,context-switches -t $TIDS -- $CMD >> $D/perfstat-$T.txt 2>&1
  done
  for p in "${PIDS[@]}"; do kill $p 2>/dev/null; done
  for p in "${PIDS[@]}"; do timeout 10 tail --pid=$p -f /dev/null || kill -9 $p 2>/dev/null; done
  exit 0
fi
cputicks() { local t=0; for p in "${PIDS[@]}"; do t=$((t + $(awk '{print $14+$15}' /proc/$p/stat))); done; echo $t; }
for P in 1 16; do
  if [ $P = 1 ]; then NREQ=${NREQ1:-4000000}; else NREQ=${NREQ16:-20000000}; fi
  for r in $(seq $RUNS); do
    F=$D/P$P-run$r.txt; : > $F
    for T in set get; do
      CMD="taskset -c $CCPU $BENCH ${BARGS[*]} -c 400 -d 64 -r 1000000 -n $NREQ -P $P --threads $CTHR -t $T --csv"
      echo "$CMD" >> $F
      c0=$(cputicks)
      ( TIMEFORMAT="client $T: real %R user %U sys %S"; time $CMD ) >> $F 2>&1
      c1=$(cputicks)
      echo "server $T: cpu_ticks=$((c1-c0)) (100/s) requests=$NREQ" >> $F
    done
  done
done
for p in "${PIDS[@]}"; do kill $p 2>/dev/null; done
for p in "${PIDS[@]}"; do timeout 10 tail --pid=$p -f /dev/null || kill -9 $p 2>/dev/null; done
# summary: label P test median min max
for P in 1 16; do for T in SET GET; do
  vals=$(grep -h "^\"$T\"" $D/P$P-run*.txt | cut -d, -f2 | tr -d '"' | sort -n | tr '\n' ' ')
  t=$(echo $T | tr A-Z a-z)
  us=$(grep -h "^server $t:" $D/P$P-run*.txt | sed 's/.*cpu_ticks=\([0-9]*\).*requests=\([0-9]*\)/\1 \2/' | awk '{printf "%.2f ", $1*10000/$2}')
  cl=$(grep -h "^client $t:" $D/P$P-run*.txt | awk '{printf "%.2f ", ($6+$8)/$4}')
  echo "$LABEL P$P $T rps: $vals| server_us_per_req: $us| client_cores: $cl"
done; done | tee $D/summary.txt
