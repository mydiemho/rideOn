#!/usr/bin/python
import pprint

from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
import csv
from datetime import datetime
import sys

class TaxiOccupancy(Model):
  taxiId = columns.Text(primary_key=True)
  date = columns.DateTime(primary_key=True, clustering_order="DESC")
  isOccupied = columns.Boolean()
  def __repr__(self):
    return 'taxiOccupancy(taxiId=%s, isOccupied=%s)' % (self.taxiId, self.isOccupied)

fileName = sys.argv[1]
with open(fileName, 'rb') as f:
    reader = csv.reader(f)
    taxiIds = list(reader)

connection.setup(['127.0.0.1'], "taxidb")

# Sync model with cql table
sync_table(TaxiOccupancy)

for id in taxiIds[0]:
    dt = datetime.now()
    TaxiOccupancy.create(taxiId=id, isOccupied=False, date=dt)
