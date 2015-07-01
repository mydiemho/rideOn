#!/usr/bin/env bash

screen ../elasticSearch/toggleOccupiedTaxi.py
screen ../kafka/bulkLocationProducer.py ../data/1locations.csv
screen ../kafka/bulkRequestProducer.py ../data/userLocations.csv