#!/usr/bin/python
import csv
import json
import sys

parent_data = []
geo_data = []
occupancy_data = []

filename = sys.argv[1]

INDEX_NAME = "taxi_index"
PARENT = "taxi"
CHILD_GEO = "taxi_geos"
CHILD_OCCUPANCY = "taxi_occupancy"
ID_FIELD = "taxiid"

def get_data(filename):
    with open(filename, "rb") as file:
        reader = csv.reader(file)
        locations = list(reader)

    for loc in locations:
        cabId = loc[0]
        latitude = loc[1]
        longitude = loc[2]

        parent_op_dict = {
            "index": {
                "_id": cabId
            }
        }

        parent_dict = {
            "taxiid": cabId
        }

        child_op_dict = {
            "create": {
                "_index": INDEX_NAME,
                "_parent": cabId,
            },
        }

        geo_dict = {
            "location": {
                "lat": latitude,
                "lon": longitude
            },
        }

        occupancy_dict = {
            "is_occupied": loc[3]
        }

        parent_data.append(parent_op_dict)
        parent_data.append(parent_dict)

        geo_data.append(child_op_dict)
        geo_data.append(geo_dict)

        occupancy_data.append(child_op_dict)
        occupancy_data.append(occupancy_dict)

    print json.dumps(parent_data)
