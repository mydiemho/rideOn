#!/usr/bin/python

# model 2: parent/child relationship

import csv
import sys

from pyelasticsearch.client import ElasticSearch

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
es = ElasticSearch(host)


def create_index():
    # create ES client, create index

    mapping = {
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

        parent_dict = {
            "index": {
                "_type": PARENT,
                "_id": cabId
            },
            "taxiid": cabId
        }

        geo_dict = {
            "index": {
                "_type": CHILD_GEO,
                "_parent": cabId
            },
            "location": {
                "lat": latitude,
                "lon": longitude
            },
        }

        occupancy_dict = {
            "index": {
                "_type": CHILD_OCCUPANCY,
                "_parent": cabId
            },
            "is_occupied": loc[3]
        }

        parent_data.append(es.index_op(parent_dict))
        geo_data.append(es.index_op(geo_dict))
        occupancy_data.append(es.index_op(occupancy_dict))


def write_es_geo():
    # initializing the documents
    # print "Bulk indexing", len(geo_data), "documents.."

    es.bulk(actions=parent_data, doc_type=PARENT, index=INDEX_NAME)
    es.refresh(INDEX_NAME)

    es.bulk(actions=geo_data, doc_type=CHILD_GEO, index=INDEX_NAME)
    es.refresh(INDEX_NAME)

    es.bulk(actions=occupancy_data, doc_type=CHILD_OCCUPANCY, index=INDEX_NAME)
    es.refresh(INDEX_NAME)

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
