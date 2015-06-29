import json
import logging
import random

import pyelasticsearch
from pyleus.storm import SimpleBolt

from kafka import KafkaClient, SimpleProducer

INDEX_NAME = 'taxi_index'
QUERY_SIZE = 10

with open("/home/ubuntu/rideOn/config/config.json", 'rb') as file:
    config = json.load(file)

# GOTCHA:
# have to include "http://" and ends with "/", else will throw error
ELASTIC_SEARCH_CLUSTER = config['es_cluster']

KAFKA_CLUSTER = config['kafka_cluster']

log = logging.getLogger("request_processing_topology.request_bolt")

es = pyelasticsearch.ElasticSearch(urls=ELASTIC_SEARCH_CLUSTER)
kafka_client = KafkaClient(hosts=KAFKA_CLUSTER)
producer = SimpleProducer(kafka_client)


class RequestProcessingBolt(SimpleBolt):
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
            "size": QUERY_SIZE,
            "query": {
                "match": {
                    "is_occupied": "0"
                }
            },
            "filter": {
                "geo_distance": {
                    "distance": "2km",
                    "location": location
                }
            }
        }  # find closest one, send response back to user, let user pick or not

        log.debug("++++++++++++++++executing search query+++++++++++++++")
        res = es.search(query, index=INDEX_NAME)

        hits = res['hits']['hits']
        hits_count = len(hits)

        log.debug("++++++++++++++++hits count: %d+++++++++++++++\n", hits_count)
        # error handle, no cab available
        if hits_count == 0:
            return

        index = random.randint(0, hits_count - 1)
        taxi_id = hits[index]['_id']

        # send to kafka
        msg = {}
        msg['taxi_id'] = taxi_id
        msg['occupancy_status'] = 1

        taxi_doc = {
            "is_occupied": "1"
        }

        taxi_type = 'taxi'

        try:
            res = es.update(index=INDEX_NAME,
                            id=taxi_id,
                            doc=taxi_doc,
                            doc_type=taxi_type,
                            retry_on_conflict=2)

            log.debug("+++++++++++++++++++updated occupancy for taxi %s++++++++++++++++++++\n", taxi_id)
            log.debug(res)
        except Exception as e:
            log.error("++++++++++FAILED TO UPDATE OCCUPANCY+++++++++")
            log.error("%s\n", str(e))

            #
            # log.debug("+++++++++++++++++++sending occupancy_update event for taxi %s++++++++++++++++++++\n", taxi_id)
            # producer.send_messages(
            #     "occupancy_update",
            #     json.dumps(msg))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/pyleus/request_bolt.log',
        format="%(message)s",
        filemode='a'
    )

    RequestProcessingBolt().run()
