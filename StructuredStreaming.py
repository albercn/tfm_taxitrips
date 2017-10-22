#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

from __future__ import print_function

import sys
import json
import datetime

from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession, Column
from pyspark.sql.types import MapType, StringType, TimestampType, IntegerType, StructField, StructType, DoubleType
from pyspark.sql.functions import from_json, regexp_replace, to_json, struct
from kafka import KafkaProducer

# Validación del número de parametros de entrada introducidos
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: Streaming.py.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

# Obtenemos la url del broker y el topic de entrada
brokers, topic = sys.argv[1:]


# Obtención de la instancia de SparkSession
# sparkSession = getSparkSessionInstance(sc.getConf())

sparkSession = SparkSession\
        .builder\
        .appName("StructuredStreamingTaxis")\
        .getOrCreate()


# Lectura del fichero con los areas Taxi_Trips_2017.csv
# Creamos el esquema del dataframe
schemaAreas = StructType([
    StructField("the_geom", StringType(), True),
    StructField("PERIMETER", StringType(), True),
    StructField("AREA", StringType(), True),
    StructField("COMAREA_", StringType(), True),
    StructField("COMAREA_ID", StringType(), True),
    StructField("AREA_NUMBER", IntegerType(), False),
    StructField("COMMUNITY", StringType(), False),
    StructField("AREA_NUM_1", IntegerType(), True),
    StructField("SHAPE_AREA", StringType(), True),
    StructField("SHAPE_LEN", StringType(), True)
])
# Lectura del fichero
areas = sparkSession.read.csv(path="hdfs://localhost:9000/areas/", header=True, schema=schemaAreas,
                            mode="DROPMALFORMED") \
    .select("AREA_NUMBER", "COMMUNITY", "the_geom")
# Creación del dataframe para cruzar con TaxiTrips por Pickup_Community_Area
pickupAreas = areas.select(
    areas["AREA_NUMBER"].alias('Pickup_Community_Area'),
    areas["COMMUNITY"].alias('Pickup_Community_Area_Name'),
    "the_geom"
)
# Creación del dataframe para cruzar con TaxiTrips por Dropoff_Community_Area
dropoffAreas = areas.select(
    areas["AREA_NUMBER"].alias('Dropoff_Community_Area'),
    areas["COMMUNITY"].alias('Dropoff_Community_Area_Name')
)


# Creamos el esquema del json
schemaJsonTaxiTrips = StructType()\
    .add("Payment Type", StringType())\
    .add("Dropoff_Census_Tract", StringType())\
    .add("Tolls", StringType())\
    .add("Trip_Total", StringType())\
    .add("Dropoff_Centroid_Latitude", StringType())\
    .add("Fare", StringType())\
    .add("Tips", StringType())\
    .add("Pickup_Census_Tract", StringType())\
    .add("Company", StringType())\
    .add("Trip_Start_Timestamp", TimestampType())\
    .add("Trip_Miles", StringType())\
    .add("Dropoff_Community_Area", StringType())\
    .add("Taxi_ID", StringType())\
    .add("Trip_ID", StringType())\
    .add("Pickup_Centroid_Latitude", StringType())\
    .add("Extras", StringType())\
    .add("Dropoff_Centroid_Location", StringType())\
    .add("Trip_Seconds", StringType())\
    .add("Pickup_Centroid_Location", StringType())\
    .add("Trip_End_Timestamp", TimestampType())\
    .add("Pickup_Community_Area", StringType())\
    .add("Dropoff_Centroid_Longitude", StringType())\
    .add("Pickup_Centroid_Longitude", StringType())

# Formato del timestamp
tripTimestampFormat = "MM/dd/yyyy hh:mm:ss a"
jsonOptions = {"timestampFormat": tripTimestampFormat}

# Create DataSet representing the stream of input lines from kafka
kst = sparkSession\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", brokers)\
        .option("subscribe", topic)\
        .load()\
        .selectExpr("CAST(value AS STRING)")

parsed = kst.select(from_json(kst.value, schemaJsonTaxiTrips, jsonOptions).alias("parsed_value"))

taxiTripsRaw = parsed.select("parsed_value.*")

taxiTripsFormated = taxiTripsRaw.select("Trip_ID",
            "Taxi_ID",
            "Company",
            "Trip_Start_Timestamp",
            "Trip_End_Timestamp",
            taxiTripsRaw["Trip_Seconds"].astype('integer').alias("Trip_Seconds"),
            taxiTripsRaw["Trip_Miles"].astype('integer').alias("Trip_Miles"),
            "Pickup_Census_Tract",
            "Dropoff_Census_Tract",
            taxiTripsRaw["Pickup_Community_Area"].astype('integer').alias("Pickup_Community_Area"),
            taxiTripsRaw["Dropoff_Community_Area"].astype('integer').alias("Dropoff_Community_Area"),
            regexp_replace(taxiTripsRaw["Fare"], '[\$,)]', '').astype('double').alias("Fare"),
            regexp_replace(taxiTripsRaw["Tips"], '[\$,)]', '').astype('double').alias("Tips"),
            regexp_replace(taxiTripsRaw["Tolls"], '[\$,)]', '').astype('double').alias("Tolls"),
            regexp_replace(taxiTripsRaw["Extras"], '[\$,)]', '').astype('double').alias("Extras"),
            regexp_replace(taxiTripsRaw["Trip_Total"], '[\$,)]', '').astype('double').alias("Trip_Total"),
            "Payment Type",
            "Pickup_Centroid_Latitude",
            "Pickup_Centroid_Longitude",
            "Pickup_Centroid_Location",
            "Dropoff_Centroid_Latitude",
            "Dropoff_Centroid_Longitude",
            "Dropoff_Centroid_Location")

taxiTripsEnrich = taxiTripsFormated.join(pickupAreas, 'Pickup_Community_Area')\
    .join(dropoffAreas, 'Dropoff_Community_Area')

taxiTripsEnrich.printSchema()

# kstJoin = kst.join(DataFrame(areasdf).rdd)

# Aplicamos la función sendPartition a cada partición de cada RDD
# kstJoin.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition()))

# Inicio
query = taxiTripsEnrich\
    .select(taxiTripsEnrich["Trip_ID"].astype('string').alias("key"),
            to_json(struct("*")).alias("value"))\
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "outTopic") \
    .option("checkpointLocation", "hdfs://localhost:9000/checkpointKafka") \
    .outputMode("Append") \
    .start()

query.awaitTermination()



