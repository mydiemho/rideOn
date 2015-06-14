#!/usr/bin/python
import json

from kafka import KafkaConsumer
from pyelasticsearch import ElasticSearch

# To consume messages

group_name = 'geo_update'
topic_name = group_name
filename = 'config.json'

consumer = KafkaConsumer(topic_name,
                         metadata_broker_list=["localhost:9092"],
                         group_id=group_name,
                         auto_commit_enable=True
                         )

es = ElasticSearch()

for message in consumer:
    # message value is raw byte string -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    parsed_msg = json.loads(message.value)
    print "consuming for topic %s", topic_name
    indexname = 'taxi_index'
    taxi_type = 'taxi'
    taxi_id = parsed_msg['data']['taxi_id']
    taxi_doc = {
        "location": {
            "lat": parsed_msg['data']['location']['latitude'],
            "lon": parsed_msg['data']['location']['longitude']
        }
    }

    res = es.update(index=indexname,
                    id=taxi_id,
                    doc=taxi_doc,
                    doc_type=taxi_type)

    print "updated taxi location for %s" % (taxi_id)
    print
