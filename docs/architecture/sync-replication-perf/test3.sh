#!/bin/bash
# Three-cluster run: namespace t3 on a,b,c (cluster-c has 8 shards, a and b have 4); t2c on a,b only.
cd /home/user/perf
./setrtt.sh 20ms
B="python3 bench.py --duration 60s"
for trial in 1 2; do
  ./setsync.sh;        $B --namespaces t3 --label t3-rtt40-async-rate10 --rate 10 --note "3 clusters, trial $trial"
  ./setsync.sh t3;     $B --namespaces t3 --label t3-rtt40-sync-rate10  --rate 10 --note "3 clusters, trial $trial"
done
./setsync.sh t2c;      $B --namespaces t2c --label t3env-2c-rtt40-sync-rate10 --rate 10 --note "2-cluster ns in 3-cluster env"
./setsync.sh;          $B --namespaces t3 --label t3-rtt40-async-closed16 --concurrency 16 --note "3 clusters"
./setsync.sh t3;       $B --namespaces t3 --label t3-rtt40-sync-closed16  --concurrency 16 --note "3 clusters"
# Uneven standbys: cluster-c 40ms further away than cluster-b (b: 20ms one-way, c: 40ms one-way).
curl -s "127.0.0.1:19299/delay?d=40ms" >/dev/null
./setsync.sh t3;       $B --namespaces t3 --label t3-uneven-sync-rate10 --rate 10 --note "b 40ms RTT, c 80ms RTT"
./setrtt.sh 20ms
./setsync.sh
echo TEST3_DONE
