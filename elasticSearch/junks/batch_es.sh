#!/bin/bash

while read line
do
    curl -XPOST http://localhost:9200/taxi_index/taxi_geos?parent -d