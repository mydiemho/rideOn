import json
import logging
import random
from kafka import KafkaClient, SimpleProducer
import pyelasticsearch

from pyleus.storm import SimpleBolt

logging.basicConfig(
       level=logging.DEBUG,
       filename='/tmp/pyleus_simple_topology.log',
       format="%(message)s",
       filemode='a',
   )

INDEX_NAME = 'taxi_index'

log = logging.getLogger("request_topology.request_bolt")
es = pyelasticsearch.ElasticSearch()
client = KafkaClient("52.8.145.247:9092,52.8.148.251:9092,52.8.158.130:9092,52.8.162.105:9092,52.8.153.92:9092")
producer = SimpleProducer(client)

class SimpleRequestBolt(SimpleBolt):

    OUTPUT_FIELDS = ['request']

    def process_tuple(self, tup):
        request = tup.values
        log.info("++++++++++++++++++RECEIVING MSG+++++++++++++++")
        log.info(request)
        log.info(request[0])

if __name__ == '__main__':
    SimpleRequestBolt().run()