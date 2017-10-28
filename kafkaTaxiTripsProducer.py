#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

import os
import time, datetime
import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

fileName = os.path.join('data_source', 'Taxi_Trips_2017.json')

infile = open(fileName, 'r')
for line in infile:
    lineJson = json.loads(line)

    if lineJson['Trip_Seconds'] == '':
        lineJson['Trip_Seconds'] = 0
    duration = datetime.timedelta(seconds=int(lineJson['Trip_Seconds']))
    t = datetime.datetime.now()
    end = t + duration

    lineJson['Trip_Start_Timestamp'] = t.strftime("%m/%d/%Y %I:%M:%S %p")
    lineJson['Trip_End_Timestamp'] = end.strftime("%m/%d/%Y %I:%M:%S %p")

    strLineJson = json.dumps(lineJson)

    producer.send(topic="rawTopic", value=strLineJson)

    time.sleep(0.1)

infile.close()
