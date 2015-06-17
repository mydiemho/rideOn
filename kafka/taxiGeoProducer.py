#!/usr/bin/python

# Kafka producer that reads the input data in a loop in order to simulate real time events
import csv
import json
import sys

from kafka import KafkaClient, SimpleProducer

class Producer():
    def __init__(self, topic, config_file, source_file):
        self.topic = topic
        self.config_file = config_file
        self.source_file = source_file

    def genData(self):
        with open(self.config_file, 'rb') as config_file:
            config = json.load(config_file)

        kafka_cluster = config['kafka_cluster']
        source_file = self.source_file

        kafka_client = KafkaClient(kafka_cluster)

        kafka_producer = SimpleProducer(kafka_client)

        with open(source_file) as f:
            reader = csv.reader(f)
            taxiLocations = list(reader)

        while True:
            for loc in taxiLocations:
                cabId = loc[0]
                latitude = loc[1]
                longitude = loc[2]
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
    if len(sys.argv) != 2:
        print "Usage: [*.py] [source-file]"
        sys.exit(0)
        # logging.basicConfig(filename='error.log',level=logging.DEBUG)

    # logger = logging.getLogger('geo_app')
    # # create file handler which logs even debug messages
    # fh = logging.FileHandler('geoupdate.log')
    # fh.setLevel(logging.INFO)
    # logger.addHandler(fh)
    producer = Producer(
        config_file='config.json',
        topic='geo_update',
        source_file=sys.argv[1]
    )

    producer.genData()
