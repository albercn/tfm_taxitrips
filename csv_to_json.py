#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

import os
import csv
import json

fileName = os.path.join('data_source/2017', 'Taxi_Trips_2017.csv')
fileNameOut = os.path.join('data_source', 'Taxi_Trips_2017.json')

infile = open(fileName, 'r')
jsonfile = open(fileNameOut, 'w')

fieldnames = ("trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds", "trip_miles",
              "pickup_census_tract", "dropoff_census_tract", "pickup_community_area", "dropoff_community_area",
              "fare", "tips", "tolls", "extras", "trip_total", "payment_type", "company", "pickup_centroid_latitude",
              "pickup_centroid_longitude", "pickup_centroid_location", "dropoff_centroid_latitude",
              "dropoff_centroid_longitude", "dropoff_centroid_location")
reader = csv.DictReader(infile, fieldnames)
for row in reader:
    json.dump(row, jsonfile)
    jsonfile.write('\n')
