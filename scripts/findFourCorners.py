#!/usr/bin/python
import sys
import os
import math
import re

directory = sys.argv[1]
print directory

# take only first line of each file and compile into a list
# cabID, lat, long, occucpancy, timestamp

data = []
minLat = 0
minLong = 0
maxLat = 0
maxLong = 0

latitudes = []
longitudes = []
def findFourCorners(directory):
    files = os.listdir(directory)
    for f in files:
        cabId = re.search('new_(.+?).txt', f).group(1)
        lines = open("trips/"+f).read().splitlines()
        for l in lines:
            vals = l.split()
            latitudes.append(float(vals[0]))
            longitudes.append(float(vals[1]))
    print "minLat = " + str(min(latitudes))
    print "minLong = " + str(min(longitudes))
    print "maxLat = " + str(max(latitudes))
    print "maxLong = " + str(max(longitudes))

findFourCorners(directory)
