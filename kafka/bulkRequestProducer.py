#!/usr/bin/python

# SOURCE: https://www.google.com/fusiontables/data?docid=1SQDRZUcWMz22pkwKSRD3-prXN8uZUbkfoXYChfE#rows:id=1

# Kafka producer that reads the input data in a loop in order to simulate real time events
import csv
import json
import random
import time
import sys

from kafka import KafkaClient, SimpleProducer


class Producer():
    def __init__(self, topic, config_file, source_file):
        self.topic = topic
        self.config_file = config_file
        self.source_file = source_file

    def genData(self, ):
        with open(self.config_file, 'rb') as config_file:
            config = json.load(config_file)

        with open(self.source_file, 'rb') as f:
            reader = csv.DictReader(f)
            locations = list(reader)

        kafka_cluster = config['kafka_cluster']
        kafka_client = KafkaClient(kafka_cluster)
        producer = SimpleProducer(kafka_client)

        kafka_client.ensure_topic_exists(self.topic)

        while True:
            for loc in locations:
                msg = {}
                latitude = float(loc['latitude'])
                longitude = float(loc['longitude'])
                msg['location'] = {
                    'latitude': latitude,
                    'longitude': longitude
                }
                producer.send_messages(self.topic, json.dumps(msg))
                print "sending %s event for lat: %f, long: %f\n" % (self.topic, latitude, longitude)
                time.sleep(0.5)  # Creating some delay


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: [*.py] [config_file] [source_file]"
        sys.exit(0)
        # logging.basicConfig(filename='error.log',level=logging.DEBUG)

    # logger = logging.getLogger('geo_app')
    # # create file handler which logs even debug messages
    # fh = logging.FileHandler('geoupdate.log')
    # fh.setLevel(logging.INFO)
    # logger.addHandler(fh)
    producer = Producer(
        config_file=sys.argv[1],
        topic='request',
        source_file=sys.argv[2]
    )

    producer.genData()
