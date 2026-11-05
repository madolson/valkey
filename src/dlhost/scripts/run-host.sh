#!/bin/bash
# usage: run-host.sh N M [host args...]  -> starts host pinned to 0-9, prints PID
N=$1; M=$2; shift 2
RUN=/tmp/chunky-dlmopen-run
rm -rf $RUN; mkdir -p $RUN
taskset -c 0-7 /tmp/chunky-dlmopen/src/valkey-dlhost --so /tmp/chunky-dlmopen/src/libvalkey-engine.so --engines $N --io-threads $M --port 10100 --rundir $RUN "$@" > $RUN/host.log 2>&1 &
PID=$!
echo $PID >> /tmp/chunky-dlmopen-pids
for i in $(seq 500); do /tmp/chunky-dlmopen-base/src/valkey-cli -p 10100 ping >/dev/null 2>&1 && break; kill -0 $PID 2>/dev/null || break; done
echo $PID
