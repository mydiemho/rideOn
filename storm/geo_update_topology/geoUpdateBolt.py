import json
import logging

import pyelasticsearch
from pyleus.storm import SimpleBolt

from kafka import KafkaClient, SimpleProducer

with open("../../config/config.json", 'rb') as file:
    config = json.load(file)

# GOTCHA:
# have to include "http://" and ends with "/", else will throw error
ELASTIC_SEARCH_CLUSTER = config['es_cluster']

KAFKA_CLUSTER = config['kafka_cluster']

log = logging.getLogger("geo_update_topology.geo_update_bolt")

es = pyelasticsearch.ElasticSearch(urls=ELASTIC_SEARCH_CLUSTER)
kafka_client = KafkaClient(hosts=KAFKA_CLUSTER)
producer = SimpleProducer(kafka_client)


class GeoUpdateBolt(SimpleBolt):
    def process_tuple(self, tup):
        request = tup.values

        # convert the extract value to a JSON object
        parsed_msg = json.loads(request[0])
        log.debug("+++++++++++++++++++RECEIVED MSG++++++++++++++++++++")
        log.debug(parsed_msg)

        indexname = 'taxi_index'
        taxi_type = 'taxi'
        taxi_id = parsed_msg['taxi_id']
        taxi_doc = {
            "location": {
                "lat": parsed_msg['location']['latitude'],
                "lon": parsed_msg['location']['longitude']
            }
        }

        try:
            res = es.update(index=indexname,
                            id=taxi_id,
                            doc=taxi_doc,
                            doc_type=taxi_type,
                            retry_on_conflict=2)

            log.debug("+++++++++++++++++++updated location for taxi %s++++++++++++++++++++", taxi_id)
            log.debug("%s\n", res)
        except Exception as e:
            log.error("++++++++++FAILED TO UPDATE GEO+++++++++")
            log.error("%s\n", str(e))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/pyleus/geo_update_bolt.log',
        format="%(message)s",
        filemode='a'
    )

    GeoUpdateBolt().run()
