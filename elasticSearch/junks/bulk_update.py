#!/usr/bin/python

import csv
import sys

from elasticsearch import Elasticsearch

# Usage: [*.py] [host] [input file]

host = sys.argv[1]
filename = sys.argv[2]

INDEX_NAME = 'taxi'
TYPE_NAME = 'taxi_geos'
ID_FIELD = 'taxiid'

bulk_data = []
es = Elasticsearch([host])


def create_index():
    # create ES client, create index

    if es.indices.exists(INDEX_NAME):
        print("deleting '%s' index..." % (INDEX_NAME))
        res = es.indices.delete(index=INDEX_NAME)
        print(" response: '%s'" % (res))
    #
    # # since we are running locally, use one shard and no replicas
    # request_body = {
    #     "settings" : {
    #         "number_of_shards": 1,
    #         "number_of_replicas": 0
    #     }
    # }

    request_body = {
        "mappings": {
            'taxi': {
                'properties': {
                    'taxiid': {'type': 'string'},
                    'location': {'type': 'geo_point'}
                },
                "_id": {
                    "path": "taxiid"
                }
            }
        }
    }

    print("creating '%s' index..." % (INDEX_NAME))
    res = es.indices.create(index=INDEX_NAME, body=request_body)
    print(" response: '%s'" % (res))


def get_data(filename):
    with open(filename, 'rb') as file:
        reader = csv.reader(file)
        locations = list(reader)

    for loc in locations:
        cabId = loc[0]
        latitude = loc[1]
        longitude = loc[2]
        op_dict = {
        "index": {
            "_index": INDEX_NAME,
            "_type": TYPE_NAME,
            "_id": cabId
        }
    }

        data_dict = {
            'taxiid': cabId,
            'location': {
                'lat': latitude,
                'lon': longitude
            }
        }
        bulk_data.append(op_dict)
        bulk_data.append(data_dict)

def write_es_geo():
    # initializing the documents
    # print "Bulk indexing", len(self.bulk_data), "documents.."
    es.bulk(INDEX_NAME, bulk_data, refresh=True)


create_index()
get_data(filename)
write_es_geo()
