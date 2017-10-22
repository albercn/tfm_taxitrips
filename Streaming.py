#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

from __future__ import print_function

import sys
import json
import datetime

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, IntegerType, StructField, StructType

from kafka import KafkaProducer

""" Función para obtener SparkSession"""
def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


"""  Función para eliminar el simbolo $ de los importes  """
def deleteSimbol(s):
    return s.replace('$', '')


""" Función para formatear los timestamp al formato ISO"""
def formatTimestamp(t):
    return datetime.datetime.strptime(t, "%m/%d/%Y %I:%M:%S %p").isoformat()


""" Función para enviar el contenidod de cada partición al topic de salida de kafka """
def sendPartition(particion):

    productor = KafkaProducer(bootstrap_servers=brokers)

    for r in particion:
        # Eliminación del simbolo $ de los importes
        r['Fare'] = deleteSimbol(r.get('Fare'))
        r['Tips'] = deleteSimbol(r.get('Tips'))
        r['Tolls'] = deleteSimbol(r.get('Tolls'))
        r['Extras'] = deleteSimbol(r.get('Extras'))
        r['Trip_Total'] = deleteSimbol(r.get('Trip_Total'))
        # Formateo de los timestamp
        r['Trip_Start_Timestamp'] = formatTimestamp(r.get('Trip_Start_Timestamp'))
        r['Trip_End_Timestamp'] = formatTimestamp(r.get('Trip_End_Timestamp'))

        productor.send('outTopic', json.dumps(r))

    productor.close()


# Validación del número de parametros de entrada introducidos
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: Streaming.py.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "StreamingTaxis")
ssc = StreamingContext(sc, 1)
"""
# Obtención de la instancia de SparkSession
sparkSes = getSparkSessionInstance(sc.getConf())

# Lectura del fichero con el historico fichero Taxi_Trips_2017.csv
# Creamos el esquema del dataframe
schemaAreas = StructType([
    StructField("the_geom", StringType(), True),
    StructField("PERIMETER", StringType(), True),
    StructField("AREA", StringType(), True),
    StructField("COMAREA_", StringType(), True),
    StructField("COMAREA_ID", StringType(), True),
    StructField("Pickup_Community_Area", IntegerType(), False),
    StructField("COMMUNITY", StringType(), False),
    StructField("AREA_NUM_1", IntegerType(), True),
    StructField("SHAPE_AREA", StringType(), True),
    StructField("SHAPE_LEN", StringType(), True)
])

areasdf = sparkSes.read.csv(path="hdfs://localhost:9000/areas/", header=True, schema=schemaAreas,
                            mode="DROPMALFORMED") \
    .select("Pickup_Community_Area", "COMMUNITY", "the_geom") \
    .cache()
"""

# Creación del Input Direct DStream de kafka leyendo del receiver de kafka
brokers, topic = sys.argv[1:]
kst = KafkaUtils\
    .createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})\
    .map(lambda m: json.loads(m[1]))


# Aplicamos la función sendPartition a cada partición de cada RDD
kst.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))

# Inicio
ssc.start()
ssc.awaitTermination()
