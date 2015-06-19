#!/usr/bin/python

# Kafka producer that sends events in a loop in order to simulate user vacating taxi
import json
import random
import sys
import time

from pyelasticsearch import ElasticSearch
from kafka import KafkaClient, SimpleProducer


class TaxiVacateSimulator():
    def __init__(self, topic, config_file):
        self.topic = topic
        self.config_file = config_file

    def genData(self, ):
        with open(self.config_file, 'rb') as config_file:
            config = json.load(config_file)

        kafka_cluster = config['kafka_cluster']
        client = KafkaClient(kafka_cluster)
        producer = SimpleProducer(client)
        es_client = ElasticSearch(config['es_cluster'])

        while True:

            # randomize # of taxis to update
            querySize = random.sample({3, 5, 7}, 1)[0]
            print "++++query size: %d" % querySize

            # construct query to randomly select an occupied taxi
            query = {
                "from": 0,
                "size": querySize,
                "query": {
                    "function_score": {
                        "filter": {
                            "term": {"is_occupied": 1}
                        },
                        "functions": [
                            {
                                "random_score": {}
                            }
                        ]
                    }
                }
            }

            print "++++++++++++executing search query++++++++++++++"
            res = es_client.search(query, index=config['index'])

            hits = res['hits']['hits']

            # no occupied taxi
            if len(hits) == 0:
                print "++++NO OCCUPIED TAXI FOUND++++++++"
                return

            print "+++++++FOUND %f taxis" % len(hits)
            print json.dumps(hits)

            taxi_data = []
            for ob in hits:
                taxi_id = ob['_id']

                dict = {
                    'taxi_id': taxi_id,
                    'is_occupied': 0
                }

                taxi_data.append(es_client.update_op(dict, id=taxi_id))

            taxi_type = 'taxi'

            json.dumps(taxi_data)

            try:
                res = es_client.bulk(
                    taxi_data,
                    doc_type=taxi_type,
                    index="taxi_index")

                print "+++++++++++++updated occupancy for %d taxis++++++++++" % len(res['items'])
                print ("%s\n" % json.dumps(res))
            except Exception as e:
                print("++++++++++FAILED TO UPDATE GEO+++++++++")
                print("%s\n", str(e))


            time.sleep(3)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: [*.py] [config_file]"
        sys.exit(0)
        # logging.basicConfig(filename='error.log',level=logging.DEBUG)

    # logger = logging.getLogger('geo_app')
    # # create file handler which logs even debug messages
    # fh = logging.FileHandler('geoupdate.log')
    # fh.setLevel(logging.INFO)
    # logger.addHandler(fh)
    producer = TaxiVacateSimulator(
        config_file=sys.argv[1],
        topic='occupancy_update'
    )

    producer.genData()
