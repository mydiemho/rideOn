#!/usr/bin/python
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
import csv
from datetime import datetime
import sys

class TaxiLocation(Model):
  taxi_id = columns.Text(primary_key=True)
  time = columns.DateTime(primary_key=True, clustering_order="DESC")
  latitude = columns.Float()
  longitude = columns.Float()
  def __repr__(self):
    return 'taxiLocation(taxiId=%s, latitude=%d, longitude=%d)' % (self.taxi_id, self.latitude, self.longitude)

fileName = sys.argv[1]
with open(fileName, 'rb') as f:
    reader = csv.reader(f)
    taxiLocations = list(reader)

connection.setup(['127.0.0.1'], "taxidb")

# Sync model with cql table
sync_table(TaxiLocation)

for loc in taxiLocations:
    dt = datetime.now()
    TaxiLocation.create(taxi_id=loc[0], latitude=loc[1], longitude=loc[2], time=dt)