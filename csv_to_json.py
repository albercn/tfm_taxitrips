#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

import os
import csv
import json

fileName = os.path.join('data_source/2017', 'Taxi_Trips_2017.csv')
fileNameOut = os.path.join('data_source', 'Taxi_Trips_2017.json')

infile = open(fileName, 'r')
jsonfile = open(fileNameOut, 'w')

fieldnames = ("Trip_ID", "Taxi_ID", "Trip_Start_Timestamp", "Trip_End_Timestamp", "Trip_Seconds", "Trip_Miles", "Pickup_Census_Tract", "Dropoff_Census_Tract", "Pickup_Community_Area", "Dropoff_Community_Area", "Fare", "Tips", "Tolls", "Extras", "Trip_Total", "Payment Type", "Company", "Pickup_Centroid_Latitude", "Pickup_Centroid_Longitude", "Pickup_Centroid_Location", "Dropoff_Centroid_Latitude", "Dropoff_Centroid_Longitude", "Dropoff_Centroid_Location")
reader = csv.DictReader(infile, fieldnames)
for row in reader:
    json.dump(row, jsonfile)
    jsonfile.write('\n')
