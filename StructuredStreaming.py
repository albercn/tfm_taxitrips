#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, TimestampType, IntegerType, StructField, StructType
from pyspark.sql.functions import from_json, regexp_replace, to_json, struct


# Validación del número de parametros de entrada introducidos
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: Streaming.py <broker_list> <inTopic> <outTopic>", file=sys.stderr)
        exit(-1)

# Obtenemos la url del broker y el topic de entrada
brokers, inTopic, outTopic = sys.argv[1:]


# Obtención de la instancia de SparkSession
# sparkSession = getSparkSessionInstance(sc.getConf())

sparkSession = SparkSession\
        .builder\
        .appName("StructuredStreamingTaxis")\
        .getOrCreate()


# Lectura del fichero con los areas Taxi_Trips_2017.csv
# Creamos el esquema del dataframe
schemaAreas = StructType([
    StructField("Area_Number", IntegerType(), False),
    StructField("Community", StringType(), False),
    StructField("Area_Centroid_Latitude", StringType(), True),
    StructField("Area_Centroid_Longitude", StringType(), True),
    StructField("The_Geom", StringType(), True)
])
# Lectura del fichero
areas = sparkSession.read.csv(path="hdfs://localhost:9000/areas/", header=True, schema=schemaAreas,
                            mode="DROPMALFORMED")
# Creación del dataframe para cruzar con TaxiTrips por Pickup_Community_Area
pickupAreas = areas.select(
    areas["Area_Number"].alias('Pickup_Community_Area'),
    areas["Community"].alias('Pickup_Community_Area_Name'),
    areas["Area_Centroid_Latitude"].alias('Pickup_Centroid_Latitude'),
    areas["Area_Centroid_Longitude"].alias('Pickup_Centroid_Longitude')
)
# Creación del dataframe para cruzar con TaxiTrips por Dropoff_Community_Area
dropoffAreas = areas.select(
    areas["Area_Number"].alias('Dropoff_Community_Area'),
    areas["Community"].alias('Dropoff_Community_Area_Name'),
    areas["Area_Centroid_Latitude"].alias('Dropoff_Centroid_Latitude'),
    areas["Area_Centroid_Longitude"].alias('Dropoff_Centroid_Longitude')
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

# Creación del DataFrame que representa el stream de viajes en taxi
kst = sparkSession\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", brokers)\
        .option("subscribe", inTopic)\
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
            taxiTripsRaw["Pickup_Community_Area"].astype('integer').alias("Pickup_Community_Area"),
            taxiTripsRaw["Dropoff_Community_Area"].astype('integer').alias("Dropoff_Community_Area"),
            regexp_replace(taxiTripsRaw["Fare"], '[\$,)]', '').astype('double').alias("Fare"),
            regexp_replace(taxiTripsRaw["Tips"], '[\$,)]', '').astype('double').alias("Tips"),
            regexp_replace(taxiTripsRaw["Tolls"], '[\$,)]', '').astype('double').alias("Tolls"),
            regexp_replace(taxiTripsRaw["Extras"], '[\$,)]', '').astype('double').alias("Extras"),
            regexp_replace(taxiTripsRaw["Trip_Total"], '[\$,)]', '').astype('double').alias("Trip_Total")
            )

# Enriquecemos el Stream con los nombres de los areas de inicio y fin
taxiTripsEnrich = taxiTripsFormated.join(pickupAreas, 'Pickup_Community_Area')\
    .join(dropoffAreas, 'Dropoff_Community_Area')

taxiTripsEnrich.printSchema()
# Inicio del aquery que escribe el resultado a kafka
queryToKafka = taxiTripsEnrich\
    .select(taxiTripsEnrich["Trip_ID"].astype('string').alias("key"),
            to_json(struct("*")).alias("value"))\
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("topic", outTopic) \
    .option("checkpointLocation", "hdfs://localhost:9000/TaxiTrips/checkpointKafka") \
    .outputMode("Append") \
    .start()

# Inicio de la query que escribe los eventos a HDFS
"""
queryToHDFS = taxiTripsRaw.writeStream \
  .format("parquet") \
  .option("path", "hdfs://localhost:9000/TaxiTrips/rawEvents") \
  .option("checkpointLocation", "hdfs://localhost:9000/TaxiTrips/checkpointHDFS") \
  .outputMode("Append") \
  .start()
"""
queryToKafka.awaitTermination()
#queryToHDFS.awaitTermination()
