#!/usr/bin/env python
#coding=utf8
import sys


def read_input(file):
    """Read input and split."""
    for line in file:
        yield line.rstrip().split('\t')


def uni_count():
    data = read_input(sys.stdin)
    for line in data:
        #if len(line) < 42:
        #    continue
        feature = line[10]
        print "%s\t%s" % (feature, 1)


def bi_count():
    data = read_input(sys.stdin)
    for line in data:
        requestId = line[2]
        site = line[29]
        category = line[35]
        location = line[36]
        if int(site) != 1:
            continue
        if category == 'FOCUS2' and int(location) in {20, 21, 22, 23, 24, 25, 26, 28, 29}:
            print "%s\t%s" % (requestId, location)


def tri_count():
    data = read_input(sys.stdin)
    for line in data:
        requestId, u, site, category, location = line[2], line[24], line[29],  line[35], line[36]
        if int(site) != 1:
            continue
        if category == 'FOCUS2' and int(location) in {20, 21, 22, 23, 24, 25, 26, 28, 29} and u != '-':
            print("%s,%s,%s" % (u, requestId, location))  # (u, requestId, location) as key


if __name__ == "__main__":
    uni_count()
	bi_count()
    tri_count()
