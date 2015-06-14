#!/usr/bin/python
import json
import random

from kafka import KafkaConsumer, KafkaClient, SimpleProducer
from pyelasticsearch import ElasticSearch

# To consume messages

GROUP_NAME = 'user_request'
INDEX_NAME = 'taxi_index'

topic_name = 'user_request'

consumer = KafkaConsumer(topic_name,
                         metadata_broker_list=["localhost:9092"],
                         group_id=GROUP_NAME,
                         auto_commit_enable=True
                         )

client = KafkaClient("localhost:9092")
producer = SimpleProducer(client)

es = ElasticSearch()

for message in consumer:
    # message value is raw byte string -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print "consuming for topic %s" % topic_name
    parsed_msg = json.loads(message.value)

    location = {
        "lat": parsed_msg['location']['latitude'],
        "lon": parsed_msg['location']['longitude']
    }

    query = {
        "from": 0,
        "size": 1,
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

    print "executing search query"
    res = es.search(query, index=INDEX_NAME)
    hits = res['hits']['hits']
    index = random.randint(0, 14)
    # print "index ", index
    taxi_id = hits[index]['_id']
    print "sending occupancy_update event for taxi %s\n" % taxi_id
    # print json.dumps(hits[index])

    # send to kafka
    msg = {}
    msg['taxi_id'] = taxi_id

    producer.send_messages(
        "occupancy_update",
        json.dumps(msg))

