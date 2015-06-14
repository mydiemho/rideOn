#!/usr/bin/python

# Kafka producer that reads the input data in a loop in order to simulate real time events
import csv
import json
import time
import sys

from kafka import KeyedProducer, KafkaClient

class Producer():
    def __init__(self, topic, key, config_file='config.json'):
        self.topic = topic
        self.key = key
        self.config_file = config_file

        with open(self.config_file, 'rb') as config_file:
            config = json.load(config_file)

        kafka_host = config['kafka']
        client = KafkaClient(kafka_host)
        self.producer = KeyedProducer(client)

        client.ensure_topic_exists(self.topic)

    def sendMsg(self, msg):
        self.producer.send(key=self.key, topic=self.topic, msg=json.dumps(msg))
        print "sent msg for topic ", self.topic
        time.sleep(0.1)  # Creating some delay

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: [*.py] [config_file] [topicname]"
        sys.exit(0)
        # logging.basicConfig(filename='error.log',level=logging.DEBUG)

    # logger = logging.getLogger('geo_app')
    # # create file handler which logs even debug messages
    # fh = logging.FileHandler('geoupdate.log')
    # fh.setLevel(logging.INFO)
    # logger.addHandler(fh)

