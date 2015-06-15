__author__ = 'myho'

from pyleus.storm import SimpleBolt

class RequestBolt(SimpleBolt):

    OUTPUT_FIELDS = ['request']

    def process_tuple(self, tup):
        request = tup.values
        print request

if __name__ == '__main__':
    RequestBolt().run()