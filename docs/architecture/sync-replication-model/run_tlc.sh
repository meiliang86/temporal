#!/bin/bash
# Usage: run_tlc.sh <max writes> <max failovers> [mutation] [variant]   (TLA2TOOLS=/path/to/tla2tools.jar)
#   variant: v1 (default: ForceDropsOld, StrictRejoin) or term-rule
set -e
W=${1:-2}; F=${2:-1}; M=${3:-none}; V=${4:-v1}
if [ "$V" = "term-rule" ]; then FDO=FALSE; SR=FALSE; TR=TRUE; else FDO=TRUE; SR=TRUE; TR=FALSE; fi
JAR=${TLA2TOOLS:-tla2tools.jar}
DIR=$(mktemp -d)
cp "$(dirname "$0")/SyncReplication.tla" $DIR/
cat > $DIR/SyncReplication.cfg <<CFG
SPECIFICATION Spec
CONSTANTS
  MaxWrites = $W
  MaxFailovers = $F
  Mutation = "$M"
  ForceDropsOld = $FDO
  StrictRejoin = $SR
  TermRule = $TR
INVARIANTS
  Durable
  OnActive
  Consistent
CFG
cd $DIR && java -XX:+UseParallelGC -Xmx${TLC_HEAP:-6g} -cp "$JAR" tlc2.TLC -workers ${TLC_WORKERS:-2} -deadlock -cleanup -config SyncReplication.cfg SyncReplication
