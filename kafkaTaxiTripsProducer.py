#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

# Importación del fichero de configuración
import taxi_trips_config as cfg

import os
import time
import datetime
import csv
import json
from kafka import KafkaProducer

# Creación de un productor de kafka
producer = KafkaProducer(bootstrap_servers=cfg.kafka_brokers)
# Lectura del fichero  con la información de los taxis de 2017 (hasta Julio)
fileName = os.path.join('data_source/2017', 'Taxi_Trips_2017.csv')
# Apertura del fichero
infile = open(fileName, 'r')

fieldnames = ("trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds", "trip_miles",
              "pickup_census_tract", "dropoff_census_tract", "pickup_community_area", "dropoff_community_area",
              "fare", "tips", "tolls", "extras", "trip_total", "payment_type", "company", "pickup_centroid_latitude",
              "pickup_centroid_longitude", "pickup_centroid_location", "dropoff_centroid_latitude",
              "dropoff_centroid_longitude", "dropoff_centroid_location")
reader = csv.DictReader(infile, fieldnames)
i = 0
for row in reader:
    # Filtrado de la cabecera del fichero
    if i != 0:

        # Inicialización a 0 del valor del campo trip_seconds cuando no esta informado.
        if row['trip_seconds'] == '':
            row['trip_seconds'] = 0

        # Captura del timestamp actual, para informar el campos trip_start_timestamp y suma al mismo
        # de la duración del viaje informada en el campo trip_seconds para informarlo como trip_end_timestamp
        duration = datetime.timedelta(seconds=int(row['trip_seconds']))
        t = datetime.datetime.utcnow()
        end = t + duration
        row['trip_start_timestamp'] = t.strftime("%m/%d/%Y %I:%M:%S %p")
        row['trip_end_timestamp'] = end.strftime("%m/%d/%Y %I:%M:%S %p")

        # Formateo del mensajea JSON
        strLineJson = json.dumps(row)

        # Envío del mensaje a kafka
        producer.send(topic=cfg.kafka_inTopic, key=row['taxi_id'], value=strLineJson)
    i = i + 1
    
    time.sleep(0.05)
# Cierre del fichero
infile.close()
