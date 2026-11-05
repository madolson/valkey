#!/bin/bash
# v4: jemalloc engine (--disable-initial-exec-tls) vs the libc engine, same session.
cd /tmp/chunky-dlmopen
export NREQ1=3000000 NREQ16=20000000 RUNS=3 OUT=$HOME/chunky/data/dlmopen/bench4
DLHOST_DIR=/tmp/chunky-dlmopen-jem/src CLUSTERCLIENT=1 ./bench.sh jem-dlhost-sa-n4-m4 dlhost 4 4 --node-ports
DLHOST_DIR=/tmp/chunky-dlmopen-jem/src ./bench.sh jem-dlhost-n4-m4 dlhost 4 4
DLHOST_DIR=/tmp/chunky-dlmopen-jem/src CLUSTERCLIENT=1 ./bench.sh jem-dlhost-sa-n2-m6 dlhost 2 6 --node-ports
CLUSTERCLIENT=1 ./bench.sh dlhost-sa-n4-m4 dlhost 4 4 --node-ports
./bench.sh cluster4-io2 cluster 4 2
echo MATRIX DONE
