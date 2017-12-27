#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

"""
Proceso para la creación del "maestro" de areas, a partir de los viajes en taxi de 2017.

"Area_Number", "Community", "Area_Centroid_Latitude", "Area_Centroid_Longitude" y "The_Geom"
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, IntegerType, StructField, StructType, DoubleType

sparkSession = SparkSession\
                .builder\
                .master("local")\
                .appName("AreasLoc") \
                .getOrCreate()

# Lectura del fichero con el historico fichero Taxi_Trips_2017.csv
# Creamos el esquema del dataframe
schemaTaxiTrips = StructType([
    StructField("Trip_ID", StringType(), False),
    StructField("Taxi_ID", StringType(), False),
    StructField("Trip_Start_Timestamp", TimestampType(), True),
    StructField("Trip_End_Timestamp", TimestampType(), True),
    StructField("Trip_Seconds", IntegerType(), True),
    StructField("Trip_Miles", DoubleType(), True),
    StructField("Pickup_Census_Tract", StringType(), True),
    StructField("Dropoff_Census_Tract", StringType(), True),
    StructField("Pickup_Community_Area", IntegerType(), True),
    StructField("Dropoff_Community_Area", IntegerType(), True),
    StructField("Fare", StringType(), True),
    StructField("Tips", StringType(), True),
    StructField("Tolls", StringType(), True),
    StructField("Extras", StringType(), True),
    StructField("Trip_Total", StringType(), True),
    StructField("Payment Type", StringType(), True),
    StructField("Company", StringType(), True),
    StructField("Pickup_Centroid_Latitude", DoubleType(), True),
    StructField("Pickup_Centroid_Longitude", DoubleType(), True),
    StructField("Pickup_Centroid_Location", StringType(), True),
    StructField("Dropoff_Centroid_Latitude", DoubleType(), True),
    StructField("Dropoff_Centroid_Longitude", DoubleType(), True),
    StructField("Dropoff_Centroid_Location", StringType(), True)
])

viajes = sparkSession.read.csv(path="file:///home/acuesta/PycharmProjects/tfm_taxitrips/data_source/2017/", header=True,
                               schema=schemaTaxiTrips, timestampFormat="MM/dd/yyyy hh:mm:ss a", mode="DROPMALFORMED")

areasLoc = viajes.filter(viajes.Company.isNotNull() & viajes.Pickup_Community_Area.isNotNull()
                         & viajes.Pickup_Census_Tract.isNull())\
    .select(
        viajes["Pickup_Community_Area"].alias("Area_Number"),
        viajes["Pickup_Centroid_Latitude"].alias("Area_Centroid_Latitude"),
        viajes["Pickup_Centroid_Longitude"].alias("Area_Centroid_Longitude")
    ).dropDuplicates()

# Creamos el esquema del dataframe
schemaAreas = StructType([
    StructField("The_Geom", StringType(), True),
    StructField("Perimeter", StringType(), True),
    StructField("area", StringType(), True),
    StructField("Comarea_", StringType(), True),
    StructField("Comarea_Id", StringType(), True),
    StructField("Area_Number", IntegerType(), False),
    StructField("Community", StringType(), False),
    StructField("Area_Num_1", IntegerType(), True),
    StructField("Shape_Area", StringType(), True),
    StructField("Shape_Len", StringType(), True)
])
# Lectura del fichero
areas = sparkSession.read.csv(path="file:///home/acuesta/PycharmProjects/tfm_taxitrips/data_source/CommAreas.csv",
                              header=False, schema=schemaAreas, mode="DROPMALFORMED", sep=";")

areasComplete = areas.join(areasLoc, "Area_Number").select("Area_Number", "Community", "Area_Centroid_Latitude",
                                                           "Area_Centroid_Longitude", "The_Geom")

areasComplete.coalesce(1).write.csv(path="file:///home/acuesta/PycharmProjects/tfm_taxitrips/data_source/areasComplete",
                                    mode='overwrite')
