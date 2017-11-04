#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

import os
import time
import datetime
import csv
import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

fileName = os.path.join('data_source/2017', 'Taxi_Trips_2017.csv')

infile = open(fileName, 'r')

fieldnames = ("trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds", "trip_miles",
              "pickup_census_tract", "dropoff_census_tract", "pickup_community_area", "dropoff_community_area",
              "fare", "tips", "tolls", "extras", "trip_total", "payment_type", "company", "pickup_centroid_latitude",
              "pickup_centroid_longitude", "pickup_centroid_location", "dropoff_centroid_latitude",
              "dropoff_centroid_longitude", "dropoff_centroid_location")
reader = csv.DictReader(infile, fieldnames)
i = 0
for row in reader:
    if i != 0:
        if row['trip_seconds'] == '':
            row['trip_seconds'] = 0
        duration = datetime.timedelta(seconds=int(row['trip_seconds']))
        t = datetime.datetime.now()
        end = t + duration

        row['trip_start_timestamp'] = t.strftime("%m/%d/%Y %I:%M:%S %p")
        row['trip_end_timestamp'] = end.strftime("%m/%d/%Y %I:%M:%S %p")

        strLineJson = json.dumps(row)

        producer.send(topic="rawTopic", value=strLineJson)
    i = i + 1
    
    time.sleep(1)

infile.close()
