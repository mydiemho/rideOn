RideOn
=================

[RideOn](http://mydiemho.link) was my big data project as part of [Insight Data Science's](http://insightdataengineering.com/) Engineering fellowship program from June 2015 through July 2015


## Intro
**RideOn** is an open-source data pipeline to simulate realtime ridesharing. It includes a map that let user search for nearby cars and visualizations that monitor the pipeline.

It makes use of the following technologies:
- Apache Zookeeper 3.4.6
- Apache Kafka 0.8.2.1
- Apache Storm 0.9.2
- Elasticsearch 1.6.0
- Flask with the following frameworks: Google Map, jQuery, Bootstrap
- Kafka-Python 0.9.2 (Kafka with Python)
- Pyleus 0.2.4 (Storm with Python)
- pyelasticsearch 1.3 (Elasticsearch with Python)
- [storm-graphite](https://github.com/verisign/storm-graphite) 0.1.4
- [statsd](https://github.com/etsy/statsd) 0.7.2
- [kafka-statsd-metrics2](https://github.com/airbnb/kafka-statsd-metrics2) 0.4.0

## Live Demo
A [live demo](http://mydiemho.link) is currently (June 2015) running.

Read further for more details about the project

## The Data

To simulate car movement, I used a dataset of mobility traces of taxi cabs in San Francisco, USA provided by [CRAWDAD](http://crawdad.org/epfl/mobility/).

To simulate user request, I used locations of businesses in San Francisco as check points.

![data](github/images/data_taxi.png)
![data](github/images/data_taxi.png)

## Pipeline Overview
![alt tag](github/images/pipeline.png)

## Realtime Processing
![alt tag](github/images/storm.png)

I used **Apache Storm** to provide real-time data processing.
- A 4-node cluster with 3 supervisors.
- 2 topologies: one to process location updates and one to process user request.
- Each topology consists of a kafka-spout and a bolt
- The Storm topology is loaded via **Pyleus**.

## Realtime Kafka and Storm monitoring


## Install And Setup:

Instructions are specific to Debian/Ubuntu.


## Presentation Deck
My presentation slides are available at http://www.slideshare.net/MsSophieHowl/my-ho-week5demo


