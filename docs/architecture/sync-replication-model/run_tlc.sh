#!/bin/bash
# Usage: run_tlc.sh <max writes> <max failovers> [mutation]   (needs tla2tools.jar: TLA2TOOLS=/path/to/jar)
set -e
W=${1:-2}; F=${2:-1}; M=${3:-none}
JAR=${TLA2TOOLS:-tla2tools.jar}
DIR=$(mktemp -d)
cp "$(dirname "$0")/SyncReplication.tla" $DIR/
cat > $DIR/SyncReplication.cfg <<CFG
SPECIFICATION Spec
CONSTANTS
  MaxWrites = $W
  MaxFailovers = $F
  Mutation = "$M"
INVARIANTS
  Durable
  OnActive
  Consistent
CFG
cd $DIR && java -XX:+UseParallelGC -Xmx${TLC_HEAP:-6g} -cp "$JAR" tlc2.TLC -workers ${TLC_WORKERS:-2} -deadlock -cleanup -config SyncReplication.cfg SyncReplication
