#!/bin/bash
# Usage: setsync.sh [namespace...]  -- enables sync replication on cluster-a for exactly these namespaces.
F=/home/user/perf/config/dynamic-a.yaml
cp /home/user/perf/config/dynamic-base.yaml $F
if [ $# -gt 0 ]; then
  echo "history.syncReplicationEnabled:" >> $F
  for ns in "$@"; do printf '  - value: true\n    constraints:\n      namespace: %s\n' "$ns" >> $F; done
fi
sleep 12   # dynamic config poll interval is 5s
