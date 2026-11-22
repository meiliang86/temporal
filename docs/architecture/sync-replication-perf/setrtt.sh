#!/bin/bash
# Usage: setrtt.sh <one-way delay, e.g. 20ms>
curl -s "127.0.0.1:17299/delay?d=$1" >/dev/null; curl -s "127.0.0.1:18299/delay?d=$1"
