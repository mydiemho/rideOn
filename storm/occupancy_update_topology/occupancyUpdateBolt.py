import json
import logging
import random

from kafka import KafkaClient, SimpleProducer
import pyelasticsearch
from pyleus.storm import SimpleBolt

# Storm bolt that toggle occupancy status to simulate user vacating taxi
# operate at the same rate as incoming user request


# GOTCHA:
# have to include "http://" and ends with "/", else will throw error
ELASTIC_SEARCH_CLUSTER = [
    "http://52.8.145.247:9200/", "http://52.8.148.251:9200/", "http://52.8.158.130:9200/", "http://52.8.162.105:9200/",
    "http://52.8.153.92:9200/"]

KAFKA_CLUSTER = "52.8.145.247:9092,52.8.148.251:9092,52.8.158.130:9092,52.8.162.105:9092,52.8.153.92:9092"

log = logging.getLogger("geo_update_topology.geo_update_bolt")

es_client = pyelasticsearch.ElasticSearch(urls=ELASTIC_SEARCH_CLUSTER)
kafka_client = KafkaClient(hosts=KAFKA_CLUSTER)
producer = SimpleProducer(kafka_client)


class OccupancyUpdateBolt(SimpleBolt):
    OUTPUT_FIELDS = ['geo_update']

    def process_tuple(self, tup):
        #ignore incoming tuple

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
        res = es_client.search(query, index="taxi_index")

        hits = res['hits']['hits']

        # no occupied taxi
        if len(hits) == 0:
            return

        taxi_id = hits[0]['_id']
        print ("++++++++++++++++++vacating taxi %s++++++++", taxi_id)
        taxi_doc = {
            "is_occupied": "0"
        }

        taxi_type = 'taxi'

        try:
            res = es_client.update(index="taxi_index",
                            id=taxi_id,
                            doc=taxi_doc,
                            doc_type=taxi_type)
            print "+++++++++++++updated occupancy++++++++++\n"
            print res
        except Exception as e:
            print("++++++++++FAILED TO UPDATE GEO+++++++++")
            print("%s\n", str(e))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/pyleus/occupancy_update_bolt.log',
        format="%(message)s",
        filemode='a'
    )

    OccupancyUpdateBolt().run()
