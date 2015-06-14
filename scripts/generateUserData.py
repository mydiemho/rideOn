#!/usr/bin/python
import sys
import os
import math
import re
import uuid
import random
import pprint
from datetime import datetime

minLat = 32.8697
minLong = -127.08143
maxLat = 50.30546
maxLong = -115.56218

# take only first line of each file and compile into a list
# cabID, lat, long, occucpancy, timestamp

data = []

def generateUserData():
    userId = str(uuid.uuid4())
    latitude = random.uniform(minLat, maxLat)
    longitude = random.uniform(minLong, maxLong)
    dt = datetime.now()
    data = [cabId, latitude, longitude, isOccupied]
    locations.append(data)

