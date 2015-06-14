__author__ = 'myho'

## to test the location search of ES
## write several geo points to ES, then return the neighbors by distance order
from elasticsearch import Elasticsearch
es = Elasticsearch('http://localhost:9200/')

def create_es():
	# by default we connect to localhost:9200
	mapping = {
		'taxi_geos': {
			'properties': {
				'taxi_id': {'type': 'long'},
				'location': {'type': 'geo_point'}
			},
			"_id" : {
				"path" : "taxi_id"
			}
		}
	}

	## create index
	try:
		es.delete_index('geos')
		print "Index 'geos' deleted!"
	except Exception as e:
		pass
	es.create_index('geos', settings={'mappings': mapping})
	print "Index 'geos' created!"
	es.refresh('geos')
