#!/usr/bin/python

# Kafka producer that sends events in a loop in order to simulate user vacating taxi
import json
import random
import time
import sys

from kafka import KafkaClient, SimpleProducer

class UserVacatingTaxiProducer():
    def __init__(self, topic, config_file):
        self.topic = topic
        self.config_file = config_file


    def genData(self, ):
        with open(self.config_file, 'rb') as config_file:
            config = json.load(config_file)

        kafka_cluster = config['kafka_cluster']
        client = KafkaClient(kafka_cluster)
        producer = SimpleProducer(client)

        while True:
            msg = {}
            msg['taxi_id'] = taxi_id
            msg['occupancy_status'] = 0

            producer.send_messages(
                "occupancy_update",
                json.dumps(msg))


            producer.send_messages(self.topic, json.dumps(msg))
            print "sending occupancy_update event for taxi %s\n" % taxi_id
            time.sleep(30)  # Creating some delay


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: [*.py] [config_file]"
        sys.exit(0)
        # logging.basicConfig(filename='error.log',level=logging.DEBUG)

    # logger = logging.getLogger('geo_app')
    # # create file handler which logs even debug messages
    # fh = logging.FileHandler('geoupdate.log')
    # fh.setLevel(logging.INFO)
    # logger.addHandler(fh)
    producer = UserVacatingTaxiProducer(
        config_file=sys.argv[1],
        topic='user_request'
    )

    producer.genData()
