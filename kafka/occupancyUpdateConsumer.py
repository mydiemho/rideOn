#!/usr/bin/python
import json

from kafka import KafkaConsumer
from pyelasticsearch import ElasticSearch

# To consume messages

group_name = 'occupancy_update'
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
    print "consuming for topic %s" % topic_name
    parsed_msg = json.loads(message.value)
    indexname = 'taxi_index'
    taxi_type = 'taxi'
    taxi_id = parsed_msg['taxi_id']
    occupancy_status = parsed_msg['occupancy_status']
    taxi_doc = {
        "is_occupied": occupancy_status
    }

    res = es.update(index=indexname,
                    id=taxi_id,
                    doc=taxi_doc,
                    doc_type=taxi_type)

    print "updated occupancy for taxi %s" % taxi_id
    print json.dumps(res)
    print
