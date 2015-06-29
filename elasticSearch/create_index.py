#!/usr/bin/python

# model 1: two separate types

import csv
import json
import sys

from pyelasticsearch.client import ElasticSearch

source = sys.argv[1]

INDEX_NAME = 'taxi_index'
TYPE = 'taxi'
ID_FIELD = 'taxi_id'

taxi_data = []
occupancy_data = []

with open("../config/config.json", 'rb') as file:
    config = json.load(file)

es = ElasticSearch(config['es_cluster'])


def create_index():
    # create ES client, create index

    mapping = {
        TYPE: {
            'properties': {
                'taxi_id': {
                    'type': 'string'
                },
                'location': {
                    'type': 'geo_point'
                },
                'is_occupied': {
                    'type': 'boolean'
                }
            }
        }
    }

    ## create index
    try:
        es.delete_index(INDEX_NAME)
        print "Index ", INDEX_NAME, " deleted!"
    except Exception as e:
        pass

    es.create_index(INDEX_NAME, settings={'mappings': mapping})
    print "Index ", INDEX_NAME, " created!"
    es.refresh(INDEX_NAME)


def get_data(filename):
    with open(filename, 'rb') as file:
        reader = csv.reader(file)
        locations = list(reader)

    for loc in locations:
        cabId = loc[0]
        latitude = loc[1]
        longitude = loc[2]

        dict = {
            'taxi_id': cabId,
            'location': {
                'lat': latitude,
                'lon': longitude
            },
            'is_occupied': loc[3]
        }

        taxi_data.append(dict)


def write_es():
    # initializing the documents
    print "Bulk indexing", len(taxi_data), "documents.."
    es.bulk_index(INDEX_NAME, TYPE, taxi_data, id_field=ID_FIELD)
    es.refresh(INDEX_NAME)

    # test usage
    print "results from ES,"
    query = {
        "from": 0, "size": 2000,
        'query': {
            "match_all": {}
        }
    }

    res = es.search(query, index=INDEX_NAME)
    print len(res['hits']['hits']), "documents found"
    print "sample result"
    print res['hits']['hits'][0]

create_index()
get_data(source)
write_es()
