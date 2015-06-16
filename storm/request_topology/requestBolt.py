import json
import logging
import random

from kafka import KafkaClient, SimpleProducer
import pyelasticsearch
from pyleus.storm import SimpleBolt

INDEX_NAME = 'taxi_index'
QUERY_SIZE = 10

# GOTCHA:
# have to include "http://" and ends with "/", else will throw error
ELASTIC_SEARCH_CLUSTER = [
    "http://52.8.145.247:9200/", "http://52.8.148.251:9200/", "http://52.8.158.130:9200/", "http://52.8.162.105:9200/",
    "http://52.8.153.92:9200/"]

KAFKA_CLUSTER = "52.8.145.247:9092,52.8.148.251:9092,52.8.158.130:9092,52.8.162.105:9092,52.8.153.92:9092"

log = logging.getLogger("request_topology.request_bolt")

es = pyelasticsearch.ElasticSearch(urls=ELASTIC_SEARCH_CLUSTER)
kafka_client = KafkaClient(hosts=KAFKA_CLUSTER)
producer = SimpleProducer(kafka_client)


class RequestBolt(SimpleBolt):
    OUTPUT_FIELDS = ['request']

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
        res = es.search(query, index=INDEX_NAME)
        hits = res['hits']['hits']
        index = random.randint(0, QUERY_SIZE - 1)
        # print "index ", index
        taxi_id = hits[index]['_id']
        # log.debug("+++++++++++++++++++sending occupancy_update event for taxi %s++++++++++++++++++++\n", taxi_id)
        # print json.dumps(hits[index])

        # send to kafka
        msg = {}
        msg['taxi_id'] = taxi_id

        taxi_doc = {
            "is_occupied": "1"
        }

        taxi_type = 'taxi'
        res = es.update(index=INDEX_NAME,
                        id=taxi_id,
                        doc=taxi_doc,
                        doc_type=taxi_type)

        log.debug("+++++++++++++++++++updated occupancy for taxi %s++++++++++++++++++++\n", taxi_id)
        log.debug(res)

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

    RequestBolt().run()
