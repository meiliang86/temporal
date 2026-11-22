#!/bin/bash
# Starts delay proxies and three Temporal clusters. One-way cross-cluster delay: $1 (default 20ms).
B=/home/user/perf/bin; L=/home/user/perf/logs; D=${1:-20ms}
mkdir -p $L
# Every cluster reaches cluster-X through X's proxy: a=17233, b=18233, c=19233.
for x in "17233 7233 17299 a" "18233 8233 18299 b" "19233 9233 19299 c"; do
  set -- $x
  nohup $B/delayproxy -listen 127.0.0.1:$1 -target 127.0.0.1:$2 -delay $D -control 127.0.0.1:$3 > $L/proxy-$4.log 2>&1 &
done
cd /home/user/perf
for c in a b c; do
  nohup $B/temporal-server --config-file config/cluster-$c.yaml --allow-no-auth start > $L/cluster-$c.log 2>&1 &
done
