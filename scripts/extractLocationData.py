#!/usr/bin/python
import csv
import json
import os
import re
import sys

directory = sys.argv[1]
outfile = sys.argv[2]
locations = []

# abboip 37.75134 -122.39488
def extract(directory):
    files = os.listdir(directory)
    for f in files:
        tmp = []
        cabId = re.search('new_(.+?).txt', f).group(1)
        lines = open(directory + "/" + f).read().splitlines()
        for l in lines:
            vals = l.split()
            latitude = vals[0]
            longitude = vals[1]
            data = cabId + ", " + latitude + ", " + longitude
            tmp.append(data)
        locations.append(tmp)

    flatten = [x for t in zip(*locations) for x in t]
    for l in flatten:
        print l


# abboip, 37.75134, -122.39488
def extract2(directory, outfile):
    files = os.listdir(directory)
    for f in files:
        tmp = []
        cabId = re.search('new_(.+?).txt', f).group(1)
        fileName = directory + "/" + f
        with open(fileName, 'rb') as file:
            lines = file.read().splitlines()
            for l in lines:
                vals = l.split()
                latitude = vals[0]
                longitude = vals[1]
                data = [cabId, latitude, longitude]
                tmp.append(data)
        locations.append(tmp)

    flatten = [x for t in zip(*locations) for x in t]
    with open(outfile, 'wb') as myfile:
        wr = csv.writer(myfile)
        wr.writerows(flatten)


# abboip, 37.75134, -122.39488
def extractOnlyInitialData(directory, outfile):
    locations = []
    files = os.listdir(directory)
    for f in files:
        cabId = re.search('new_(.+?).txt', f).group(1)
        fileName = directory + "/" + f
        with open(fileName, 'rb') as file:
            lines = file.read().splitlines()
            vals = lines[0].split()
            latitude = vals[0]
            longitude = vals[1]
            isOccupied = vals[2]
            data = [cabId, latitude, longitude, isOccupied]
            locations.append(data)

    with open(outfile, 'wb') as myfile:
        wr = csv.writer(myfile)
        wr.writerows(locations)

extractOnlyInitialData(directory, outfile)