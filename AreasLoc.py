#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, IntegerType, StructField, StructType, DoubleType

sparkSession = SparkSession\
                .builder\
                .master("local")\
                .appName("IngestaHistoricoTaxi") \
                .getOrCreate()
#                .config("spark.sql.warehouse.dir", '/home/bigdata/opt/hive/warehouse') \
#                .enableHiveSupport()\
#                .getOrCreate()

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

viajes = sparkSession.read.csv(path="file:///home/albercn/PycharmProjects/TFM_TaxiTrips/data_source/2017/", header=True,
                               schema=schemaTaxiTrips, timestampFormat="MM/dd/yyyy hh:mm:ss a", mode="DROPMALFORMED")

areasdf = viajes.filter(viajes.Company.isNotNull() & viajes.Pickup_Community_Area.isNotNull()
                         & viajes.Pickup_Census_Tract.isNull())\
    .select("Pickup_Community_Area", "Pickup_Centroid_Location").dropDuplicates()

areasdf.coalesce(1).write.csv(path="file:///home/albercn/PycharmProjects/TFM_TaxiTrips/data_source/areasLoc", mode='overwrite')


