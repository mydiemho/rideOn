#!/usr/bin/env bash

screen -d -m -S exitTaxi ../elasticSearch/toggleOccupiedTaxi.py
screen -d -m -S geo ../kafka/bulkLocationProducer.py ../data/1locations.csv
screen -d -m -S request ../kafka/bulkRequestProducer.py ../data/userLocations.csv