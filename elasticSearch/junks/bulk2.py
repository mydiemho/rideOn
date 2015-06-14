#!/usr/bin/python

# model 1: two separate types

import csv
import sys

from pyelasticsearch.client import ElasticSearch

host = sys.argv[1]
filename = sys.argv[2]

INDEX_NAME = "taxi"
GEO_TYPE = "taxi_geos"
OCCUPANCY_TYPE = "taxi_occupancy"
ID_FIELD = "taxiid"

geo_data = []
occupancy_data = []
es = ElasticSearch(host)


def create_index():
    # create ES client, create index

    mapping = {
        GEO_TYPE: {
            "properties": {
                "taxiid": {"type": "string"},
                "location": {
                    "type": "geo_point"
                },
            },
            "_id": {
                "path": "taxiid"
            }
        },
        OCCUPANCY_TYPE: {
            "properties": {
                "taxiid": {"type": "string"},
                "is_occupied": {"type": "boolean"}
            },
            "_id": {
                "path": "taxiid"
            }
        }
    }

    ## create index
    try:
        es.delete_index(INDEX_NAME)
        print "Index ", INDEX_NAME, " deleted!"
    except Exception as e:
        pass

    es.create_index(INDEX_NAME, settings={"mappings": mapping})
    print "Index ", INDEX_NAME, " created!"
    es.refresh(INDEX_NAME)

def get_data(filename):
    with open(filename, "rb") as file:
        reader = csv.reader(file)
        locations = list(reader)

    for loc in locations:
        cabId = loc[0]
        latitude = loc[1]
        longitude = loc[2]

        geo_dict = {
            "taxiid": cabId,
            "location": {
                "lat": latitude,
                "lon": longitude
            }
        }

        occupancy_dict = {
            "taxiid": cabId,
            "is_occupied": loc[3]
        }

        geo_data.append(geo_dict)
        occupancy_data.append(occupancy_dict)

def write_es_geo():
    # initializing the documents
    print "Bulk indexing", len(geo_data), "documents.."
    es.bulk_index(INDEX_NAME, GEO_TYPE, geo_data, id_field=ID_FIELD)
    es.refresh(INDEX_NAME)

    es.bulk_index(INDEX_NAME, OCCUPANCY_TYPE, occupancy_data, id_field=ID_FIELD)
    es.refresh(INDEX_NAME)

    # test usage
    print "results from ES,"
    query = {
        "from" : 0, "size" : 2000,
        "query": {
             "match_all" : { }
         }
     }
    res =  es.search(query, index=INDEX_NAME)
    print len(res["hits"]["hits"]), "documents found"
    print "sample result"
    print res["hits"]["hits"][0]

create_index()
get_data(filename)
write_es_geo()
