#!/bin/bash
# Test 1: one global namespace (t1), async vs sync.
cd /home/user/perf
B="python3 bench.py --namespaces t1 --duration 60s"
./setrtt.sh 20ms
for trial in 1 2; do
  ./setsync.sh;    $B --label t1-rtt40-async-rate10 --rate 10 --note "trial $trial"
  ./setsync.sh t1; $B --label t1-rtt40-sync-rate10  --rate 10 --note "trial $trial"
done
./setsync.sh;    $B --label t1-rtt40-async-closed16 --concurrency 16
./setsync.sh t1; $B --label t1-rtt40-sync-closed16  --concurrency 16
./setrtt.sh 0ms
./setsync.sh;    $B --label t1-rtt0-async-rate10 --rate 10
./setsync.sh t1; $B --label t1-rtt0-sync-rate10  --rate 10
./setsync.sh;    $B --label t1-rtt0-async-closed16 --concurrency 16
./setsync.sh t1; $B --label t1-rtt0-sync-closed16  --concurrency 16
./setrtt.sh 20ms
./setsync.sh
echo TEST1_DONE
