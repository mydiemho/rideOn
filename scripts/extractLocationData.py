#!/usr/bin/python
import csv
import json
import os
import re
import sys
import itertools
import cPickle as pickle

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


# abboip, 37.75134, -122.39488;
def extract3(directory, outfile):
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


    # [ [['abboip', 37.75134, -122.39488], [..]], [[...], [...],
    flatten = [filter(lambda p: p is not None, pair) for pair in itertools.izip_longest(*locations)]

    with open(outfile, "wb") as file:
        pickle.dump(flatten, file)

# abboip, 37.75134, -122.39488;
def extractToCsv(directory, outfile):
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


    # [ [['abboip', 37.75134, -122.39488], [..]], [[...], [...],
    flatten = [filter(lambda p: p is not None, pair) for pair in itertools.izip_longest(*locations)]
    flatten2 = zip(*flatten)


    with open(outfile, "wb") as file:
        wr = csv.writer(file)
        wr.writerows(flatten2)

# abboip, 37.75134, -122.39488;
def extractToJson(directory, outfile):
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
                data = {
                    "taxi_id": cabId,
                    "lat": latitude,
                    "lon": longitude
                }
                tmp.append(data)
        locations.append(tmp)


    # [ [['abboip', 37.75134, -122.39488], [..]], [[...], [...],
    flatten = (filter(lambda p: p is not None, pair) for pair in itertools.izip_longest(*locations))

    with open(outfile, 'wb') as f:
        json.dump(flatten, f)


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


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: [*.py] inputDirectory, outFile"
        sys.exit(0)
        # logging.basicConfig(filename='error.log',level=logging.DEBUG)

    directory = sys.argv[1]
    outfile = sys.argv[2]
    extractToCsv(directory, outfile)
