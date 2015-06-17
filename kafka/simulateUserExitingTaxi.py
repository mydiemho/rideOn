#!/usr/bin/python

# Kafka producer that sends events in a loop in order to simulate user vacating taxi
import json
import sys

from pyelasticsearch import ElasticSearch

from kafka import KafkaClient, SimpleProducer
import time


class UserVacatingTaxiProducer():
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
            # construct query to randomly select an occupied taxi
            query = {
                "from": 0,
                "size": 1,
                "query": {
                    "function_score": {
                        "filter": {
                            "term": {"is_occupied": "1"}
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
                return

            taxi_id = hits["_id"]
            print ("++++++++++++++++++vacating taxi %s++++++++", taxi_id)
            taxi_doc = {
                "is_occupied": "0"
            }

            taxi_type = 'taxi'
            res = es_client.update(index=config['index'],
                            id=taxi_id,
                            doc=taxi_doc,
                            doc_type=taxi_type)
            print "+++++++++++++updated occupancy++++++++++"
            print res

            # msg = {}
            # msg['taxi_id'] = taxi_id
            # msg['occupancy_status'] = 0
            #
            # producer.send_messages(
            #     self.topic,
            #     json.dumps(msg))
            #
            # print "sending occupancy_update event for taxi %s\n" % taxi_id

            time.sleep(5)

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
    producer = UserVacatingTaxiProducer(
        config_file=sys.argv[1],
        topic='occupancy_update'
    )

    producer.genData()
