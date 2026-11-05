#!/bin/bash
# v3: slot-aware client against the host (--node-ports), plus same-session
# reruns of the mixed-pipeline host and the clusters. Servers 0-7, client 8-13.
cd /tmp/chunky-dlmopen
export NREQ1=3000000 NREQ16=20000000 RUNS=3 OUT=$HOME/chunky/data/dlmopen/bench3
CLUSTERCLIENT=1 ./bench.sh dlhost-sa-n2-m4 dlhost 2 4 --node-ports
CLUSTERCLIENT=1 ./bench.sh dlhost-sa-n2-m6 dlhost 2 6 --node-ports
CLUSTERCLIENT=1 ./bench.sh dlhost-sa-n4-m4 dlhost 4 4 --node-ports
CLUSTERCLIENT=1 ./bench.sh dlhost-sa-n4-m3 dlhost 4 3 --node-ports
./bench.sh dlhost-n4-m6 dlhost 4 6
./bench.sh dlhost-n4-m4 dlhost 4 4
./bench.sh cluster2-io4 cluster 2 4
./bench.sh cluster4-io2 cluster 4 2
echo MATRIX DONE
