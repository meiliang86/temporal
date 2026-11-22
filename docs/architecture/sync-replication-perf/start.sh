#!/bin/bash
# Starts the delay proxies and both Temporal clusters. One-way cross-cluster delay: $1 (default 20ms).
set -e
B=/home/user/perf/bin; L=/home/user/perf/logs; D=${1:-20ms}
mkdir -p $L
# cluster-b reaches cluster-a through 17233, cluster-a reaches cluster-b through 18233.
nohup $B/delayproxy -listen 127.0.0.1:17233 -target 127.0.0.1:7233 -delay $D -control 127.0.0.1:17299 > $L/proxy-a.log 2>&1 &
nohup $B/delayproxy -listen 127.0.0.1:18233 -target 127.0.0.1:8233 -delay $D -control 127.0.0.1:18299 > $L/proxy-b.log 2>&1 &
cd /home/user/perf
nohup $B/temporal-server --config-file config/cluster-a.yaml --allow-no-auth start > $L/cluster-a.log 2>&1 &
nohup $B/temporal-server --config-file config/cluster-b.yaml --allow-no-auth start > $L/cluster-b.log 2>&1 &
