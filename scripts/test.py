#!/usr/bin/python
import csv
import pprint
import sys

fileName = sys.argv[1]
with open(fileName, 'rb') as f:
    reader = csv.reader(f)
    taxiLocations = list(reader)

for loc in taxiLocations:
    print loc[0] + loc[1] + loc[2]