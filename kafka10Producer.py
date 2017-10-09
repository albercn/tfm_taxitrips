#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

import os
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

fileName = os.path.join('data_source', 'Taxi_Trips_2017.json')

infile = open(fileName, 'r')
for line in infile:
    producer.send("rawTopic", line)
    time.sleep(0.5)

infile.close()