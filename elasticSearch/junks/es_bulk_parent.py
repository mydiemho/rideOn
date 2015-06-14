#!/usr/bin/python

# model 2: parent/child relationship

import csv
import sys

from elasticsearch import Elasticsearch

host = sys.argv[1]
filename = sys.argv[2]

INDEX_NAME = "taxi_index"
PARENT = "taxi"
CHILD_GEO = "taxi_geos"
CHILD_OCCUPANCY = "taxi_occupancy"
ID_FIELD = "taxiid"

parent_data = []
geo_data = []
occupancy_data = []
es = Elasticsearch([host])


def create_index():
    # create ES client, create index

    request_body = {
        "mappings": {
            PARENT: {
                "properties": {
                    "taxiid": {
                        "type": "string"
                    }
                },
                "_id": {
                    "path": "taxiid"
                }
            },
            CHILD_GEO: {
                "properties": {
                    "location": {
                        "type": "geo_point"
                    },
                },
                "_parent": {
                    "type": PARENT
                }
            },
            CHILD_OCCUPANCY: {
                "properties": {
                    "is_occupied": {
                        "type": "boolean"
                    }
                },
                "_parent": {
                    "type": PARENT
                }
            }
        }

    ## create index
    try:
        es.indices.delete(INDEX_NAME)
        print "Index ", INDEX_NAME, " deleted!"
    except Exception as e:
        pass

    res = es.indices.create(index=INDEX_NAME, body=request_body, refresh=True)
    print(" response: '%s'" % (res))


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


def write_es_geo():
    # initializing the documents
    # print "Bulk indexing", len(geo_data), "documents.."
    es.bulk(body=parent_data, index=INDEX_NAME, doc_type=PARENT, refresh=True)
    es.bulk(body=geo_data, index=INDEX_NAME, doc_type=CHILD_GEO, refresh=True)
    es.bulk(body=parent_data, index=INDEX_NAME, doc_type=CHILD_OCCUPANCY, refresh=True)

    # test usage
    print "results from ES,"
    query = {
        "from": 0, "size": 2000,
        "query": {
            "match_all": {}
        }
    }
    res = es.search(query, index=INDEX_NAME)
    print len(res["hits"]["hits"]), "documents found"
    print "sample result"
    print res["hits"]["hits"][0]


create_index()
get_data(filename)
write_es_geo()
