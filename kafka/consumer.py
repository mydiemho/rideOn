import json

from kafka import KafkaClient, KafkaConsumer
from pyelasticsearch.client import ElasticSearch

# !/usr/bin/python

__author__ = 'myho'

class Consumer():
    def __init__(self, group_name, topic_name, timeout=60, filename='config.json'):
        with open(filename, 'rb') as config_file:
            self.config = json.load(config_file)

        self.es_host = self.config["elastic_search"]
        self.kafka_host = self.config["kafka"]

        self.group_name = group_name
        self.topic_name = topic_name
        self.time_out = timeout

        self.kafka = KafkaClient(self.kafka_host)
        self.kafka.ensure_topic_exists(self.topic)

        self.es = ElasticSearch(self.es_host)

    def run(self):
        consumer = KafkaConsumer(self.topic_name,
                                 metadata_broker_list=["localhost:9092"],
                                 group_id=self.group_name,
                                 auto_commit_enable=True
                                 )

        for message in consumer:
            parsed_msg = json.loads(message.value)
            self.consume_msg(parsed_msg)
            print "blah"

    def consume_msg(self, parsed_msg):
        # will be override
        return


if __name__ == "__main__":
    pass
