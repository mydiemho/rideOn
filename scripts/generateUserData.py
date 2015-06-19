#!/usr/bin/python
import csv
import sys

# abboip, 37.75134, -122.39488
import time


def extract(input, outfile):
    locations = []
    with open(input, 'rb') as file:
        reader = csv.DictReader(file)
        source = list(reader)

    locations.append(['latitude', 'longitude'])
    for row in source:
        latitude = float(row['latitude'])
        longitude = float(row['longitude'])
        data = [latitude, longitude]
        locations.append(data)

    with open(outfile, 'wb') as myfile:
        wr = csv.writer(myfile)
        wr.writerows(locations)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: [*.py] input, outFile"
        sys.exit(0)

    input = sys.argv[1]
    outfile = sys.argv[2]
    extract(input, outfile)
