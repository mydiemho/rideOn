import csv
from elasticsearch import Elasticsearch
import sys


class GenerateTaxi():

    def __init__(self, filename):
        self.ES_HOST = {"host" : "localhost", "port" : 9200}
        self.INDEX_NAME = 'taxi'
        self.TYPE_NAME = 'taxi_geos'
        self.ID_FIELD = 'taxiid'
        self.filename = filename

    def create_index(self):
        # create ES client, create index
        es = Elasticsearch(hosts = [self.ES_HOST])

        if es.indices.exists(self.INDEX_NAME):
            print("deleting '%s' index..." % (self.INDEX_NAME))
            res = es.indices.delete(index = self.INDEX_NAME)
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
            "mappings" : {
                'taxi': {
                    'properties': {
                        'taxiid': {'type': 'long'},
                        'location': {'type': 'geo_point'}
                    },
                    "_id" : {
                        "path" : "taxiid"
                    }
                }
            }
        }

        print("creating '%s' index..." % (self.INDEX_NAME))
        res = es.indices.create(index = self.INDEX_NAME, body = request_body)
        print(" response: '%s'" % (res))

    def get_data(self):
        with open(self.filename, 'rb') as file:
            reader = csv.reader(file)
            locations = list(reader)

        locations_data = []
        for loc in locations:
            cabId = loc[0]
            latitude = loc[1]
            longitude = loc[2]
            data = {
                'taxiid' : cabId,
                'location' : {
                    'lat' : latitude,
                    'lon' : longitude
                }
            }
            locations_data.append(data)

        self.bulk_data = locations_data

    def write_es_geo(self):
        # try to connect with ES and delete the index
		es = Elasticsearch(host = [self.ES_HOST])

        # initializing the documents
        # print "Bulk indexing", len(self.bulk_data), "documents.."
        es.bulk(self.INDEX_NAME, self.bulk_data)


    if __name__ == "__main__":
        if len(sys.argv)==2:
            filename = sys.argv[1]
        else:
            print "Usage: [*.py] [input file]"
            sys.exit(0)

        g = GenerateTaxi(filename=filename)
        g.create_index()
        g.get_data()
        g.write_es_geo()

