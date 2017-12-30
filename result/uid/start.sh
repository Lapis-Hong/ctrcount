#!/bin/bash
file=$1.stat
if [ $# -ne 1 ]; then
    echo "Usage: pass file prefix date param"
    exit
fi
cat $file | tail -n +2 | sort -k2,2n | python deep_user.py

