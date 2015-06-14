#!/usr/bin/python

import csv
import sys

from pyelasticsearch.client import ElasticSearch

host = sys.argv[1]
filename = sys.argv[2]

INDEX_NAME = 'taxi'
TYPE_NAME = 'taxi_occupancy'
ID_FIELD = 'taxiid'

bulk_data = []
es = ElasticSearch(host)


def create_index():
    # create ES client, create index

    mapping = {
        TYPE_NAME: {
            'properties': {
                'taxiid': {'type': 'string'},
                'is_occupied': {'type': 'boolean'}
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

        data_dict = {
            'taxiid': cabId,
            'location': {
                'lat': latitude,
                'lon': longitude
            }
        }
        bulk_data.append(data_dict)

def write_es_geo():
    # initializing the documents
    print "Bulk indexing", len(bulk_data), "documents.."
    es.bulk_index(INDEX_NAME, TYPE_NAME, bulk_data, id_field=ID_FIELD)
    es.refresh(INDEX_NAME)

    # test usage
    print "results from ES,"
    query = {
        "from" : 0, "size" : 2000,
        'query': {
             "match_all" : { }
         }
     }
    res =  es.search(query, index=INDEX_NAME)
    print len(res['hits']['hits']), "documents found"
    print "sample result"
    print res['hits']['hits'][0]

create_index()
get_data(filename)
write_es_geo()
