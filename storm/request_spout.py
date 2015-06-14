__author__ = 'myho'

# accept user request: location and rating requirement
from pyleus.storm import Spout

class UserRequestSpout(Spout):

    OUTPUT_FIELDS = ['location', 'rating']

    def next_tuple(self):

        self.emit((""))
