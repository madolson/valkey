#!/usr/bin/env python3
"""Connections one redis-py RedisCluster client process opens.
usage: conncount.py PORT THREADS SECONDS
Prints, per node, the connections the client's pool created, then the total."""
import random
import sys
import threading
import time

import redis
from redis.cluster import RedisCluster

port, nthreads, secs = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
rc = RedisCluster(host="127.0.0.1", port=port)
stop = time.time() + secs


def work():
    while time.time() < stop:
        k = "key:%d" % random.randrange(1000000)
        rc.set(k, "v")
        rc.get(k)


ts = [threading.Thread(target=work) for _ in range(nthreads)]
for t in ts:
    t.start()
time.sleep(secs / 2)
open("/tmp/sno-conncount.ready", "w").write("1")  # the shell counts sockets now
for t in ts:
    t.join()
tot = 0
for n in rc.get_nodes():
    c = n.redis_connection.connection_pool._created_connections
    tot += c
    print("node %s created_connections=%d" % (n.name, c))
print("redis-py %s threads=%d nodes=%d total_connections=%d" % (redis.__version__, nthreads, len(rc.get_nodes()), tot))
