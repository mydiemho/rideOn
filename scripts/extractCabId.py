#!/usr/bin/python
import sys
import os
import re
import csv

directory = sys.argv[1]
outFileName = sys.argv[2]

occupancy = []

# rows of single ids
def extract(directory):
    files = os.listdir(directory)
    for f in files:
        cabId = re.search('new_(.+?).txt', f).group(1)
        occupancy.append(cabId)

    with open(outFileName, 'wb') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        tmp = zip(occupancy)
        wr.writerows(tmp)

# one row of comma separated ids
def extract2(directory):
    files = os.listdir(directory)
    for f in files:
        cabId = re.search('new_(.+?).txt', f).group(1)
        occupancy.append(cabId)

    with open(outFileName, 'wb') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        wr.writerow(occupancy)

extract2(directory)
