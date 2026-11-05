#!/bin/bash
# Full matrix, one session. Servers 0-7, client 8-13 (6 threads).
cd /tmp/chunky-dlmopen
export NREQ1=3000000 NREQ16=20000000 RUNS=3
S=/tmp/chunky-dlmopen-base/src/valkey-server
L=/tmp/chunky-dlmopen/src/valkey-server
./bench.sh single-io4 single 4 $S
./bench.sh single-io6 single 6 $S
./bench.sh single-io8 single 8 $S
./bench.sh single-libc-io6 single 6 $L
./bench.sh cluster2-io4 cluster 2 4
./bench.sh cluster4-io2 cluster 4 2
./bench.sh dlhost-n1-m4 dlhost 1 4
./bench.sh dlhost-n2-m4 dlhost 2 4
./bench.sh dlhost-n2-m6 dlhost 2 6
./bench.sh dlhost-n4-m2 dlhost 4 2
./bench.sh dlhost-n4-m4 dlhost 4 4
echo MATRIX DONE
