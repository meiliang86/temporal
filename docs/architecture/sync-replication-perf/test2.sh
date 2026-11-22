#!/bin/bash
# Test 2: four global namespaces on the same cluster pair, loaded at the same time.
# Baseline: all async. Mixed: m-sync is sync, the other three async.
cd /home/user/perf
NS=m-sync,m-async-1,m-async-2,m-async-3
B="python3 bench.py --namespaces $NS --duration 60s"
./setrtt.sh 20ms
for trial in 1 2; do
  ./setsync.sh;        $B --label t2-rtt40-allasync-rate5 --rate 5 --note "trial $trial"
  ./setsync.sh m-sync; $B --label t2-rtt40-mixed-rate5    --rate 5 --note "trial $trial"
done
./setsync.sh;        $B --label t2-rtt40-allasync-closed8 --concurrency 8
./setsync.sh m-sync; $B --label t2-rtt40-mixed-closed8    --concurrency 8
./setsync.sh
echo TEST2_DONE
