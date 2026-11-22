#!/bin/bash
# Usage: setrtt.sh <one-way delay, e.g. 20ms>
for p in 17299 18299 19299; do curl -s "127.0.0.1:$p/delay?d=$1" 2>/dev/null; done | sort -u
