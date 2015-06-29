#!/usr/bin/python

# Kafka producer that reads the input data in a loop in order to simulate real time events
import csv
import json
import sys

from .. config import config
from kafka import KafkaClient, SimpleProducer


class Producer():
    def __init__(self, topic, source_file):
        self.topic = topic
        self.source_file = source_file

    def genData(self):

        with open(self.source_file) as f:
            reader = csv.DictReader(f)
            taxiLocations = list(reader)

        kafka_cluster = config.kafka_cluster
        kafka_client = KafkaClient(kafka_cluster)
        kafka_producer = SimpleProducer(kafka_client)

        for loc in taxiLocations:
            print loc.keys()
            cabId = loc["taxi_id"]
            latitude = float(loc['latitude'])
            longitude = float(loc['longitude'])
            msg = {}
            msg['taxi_id'] = cabId
            location = {
                'latitude': latitude,
                'longitude': longitude
            }
            msg['location'] = location
            kafka_producer.send_messages(self.topic, json.dumps(msg))
            print "sending location update for taxi %s" % cabId


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: [*.py] [source-file]"
        sys.exit(0)
        # logging.basicConfig(filename='error.log',level=logging.DEBUG)

    # logger = logging.getLogger('geo_app')
    # # create file handler which logs even debug messages
    # fh = logging.FileHandler('geoupdate.log')
    # fh.setLevel(logging.INFO)
    # logger.addHandler(fh)
    producer = Producer(
        topic='location',
        source_file=sys.argv[1]
    )

    producer.genData()
