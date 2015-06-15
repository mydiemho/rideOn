import logging

__author__ = 'myho'

from pyleus.storm import SimpleBolt

log = logging.getLogger("logging_example.request_bolt")

class RequestBolt(SimpleBolt):

    OUTPUT_FIELDS = ['request']

    def process_tuple(self, tup):
        request = tup.values
        log.info(request)

if __name__ == '__main__':
    RequestBolt().run()