import json
import logging
import random

from kafka import KafkaClient, SimpleProducer
import pyelasticsearch
from pyleus.storm import SimpleBolt
import sys

log = logging.getLogger("request_processing_topology.request_bolt")

class RequestBolt(SimpleBolt):
    OUTPUT_FIELDS = ['request']

    def __init__(self, config_file):
        with open(self.config_file, 'rb') as config_file:
            config = json.load(config_file)

        # GOTCHA:
        # For es cluster, have to include "http://" and ends with "/", else will throw error
        self.es = pyelasticsearch.ElasticSearch(urls=config['es_cluster'])
        self.kafka_client = KafkaClient(hosts=config['kafka_cluster'])
        self.producer = SimpleProducer(self.kafka_client)

        self.index_name = config['index']

    def process_tuple(self, tup):
        request = tup.values

        # convert the extract value to a JSON object
        parsed_msg = json.loads(request[0])
        log.debug("++++++++++++++++++RECEIVING MSG+++++++++++++++")
        log.debug(parsed_msg['location'])

        location = {
            "lat": parsed_msg['location']['latitude'],
            "lon": parsed_msg['location']['longitude']
        }

        query = {
            "from": 0,
            "size": 10,
            "query": {
                "match": {
                    "is_occupied": "0"
                }
            },
            "sort": [
                {
                    "_geo_distance": {
                        "location": location,
                        "order": "asc",
                        "unit": "km"
                    }
                }
            ]
        }

        # find closest one, send response back to user, let user pick or not

        log.debug("++++++++++++++++executing search query+++++++++++++++")
        res = self.es.search(query, index=self.index_name)
        hits = res['hits']['hits']

        log.debug("++++++++++++++++hits count: %d+++++++++++++++", len(hits))

        # error handle, no cab available
        if len(hits) == 0:
            return

        index = random.randint(0, hits - 1)
        taxi_id = hits[index]['_id']

        # send to kafka
        msg = {}
        msg['taxi_id'] = taxi_id
        msg['occupancy_status'] = 1

        # taxi_doc = {
        #     "is_occupied": "1"
        # }
        #
        # taxi_type = 'taxi'
        # res = es.update(index=INDEX_NAME,
        #                 id=taxi_id,
        #                 doc=taxi_doc,
        #                 doc_type=taxi_type)
        #
        # log.debug("+++++++++++++++++++updated occupancy for taxi %s++++++++++++++++++++\n", taxi_id)
        # log.debug(res)

        log.debug("+++++++++++++++++++sending occupancy_update event for taxi %s++++++++++++++++++++\n", taxi_id)
        self.producer.send_messages(
            "occupancy_update",
            json.dumps(msg))


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print "Usage: [*.py] [config_file]"
        sys.exit(0)

    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/pyleus/request_bolt.log',
        format="%(message)s",
        filemode='a'
    )

    bolt = RequestBolt(
        config = sys.argv[1]
    )

    bolt.run()
