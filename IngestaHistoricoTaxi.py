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

viajes = sparkSession.read.csv(path="data_source/Taxi_Trips_2017.csv", header=True, schema=schemaTaxiTrips, timestampFormat="MM/dd/yyyy hh:mm:ss a", mode="DROPMALFORMED")

#viajes.show(viajes.count())

viajesdf = viajes.filter(viajes.Company.isNotNull()
                        & viajes.Pickup_Community_Area.isNotNull())\
    .select("Trip_ID", "Taxi_ID", "Company",
    F.date_format("Trip_Start_Timestamp", "yyyy.MM.dd").alias("Trip_Start_Date"),
    F.date_format("Trip_Start_Timestamp", "HH").astype('integer').alias("Trip_Start_Hour"), "Trip_Seconds", "Trip_Miles",
    "Pickup_Census_Tract", "Dropoff_Census_Tract", "Pickup_Community_Area", "Dropoff_Community_Area",
    F.regexp_replace(viajes["Fare"], '[\$,)]', '').astype('double').alias("Fare"),
    F.regexp_replace(viajes["Tips"], '[\$,)]', '').astype('double').alias("Tips"),
    F.regexp_replace(viajes["Tolls"], '[\$,)]', '').astype('double').alias("Tolls"),
    F.regexp_replace(viajes["Extras"], '[\$,)]', '').astype('double').alias("Extras"),
    F.regexp_replace(viajes["Trip_Total"], '[\$,)]', '').astype('double').alias("Trip_Total"),
    "Payment Type", "Pickup_Centroid_Latitude",
    "Pickup_Centroid_Longitude", "Pickup_Centroid_Location", "Dropoff_Centroid_Latitude", "Dropoff_Centroid_Longitude",
    "Dropoff_Centroid_Location")

#viajesdf.show(viajesdf.count())

#agrupado = viajesdf.groupBy("Taxi_ID", "Company", "Trip_Start_Date", "Trip_Start_Hour", "Pickup_Community_Area", "Dropoff_Community_Area")\
#    .agg({"Fare": "sum", "Tips": "sum", "Tolls": "sum", "Extras": "sum", "Trip_Total": "sum", "Trip_ID": "count"})\
#    .orderBy("count(Trip_ID)", ascending=False)

# Agrupación por empresa, fecha, hora y zona
agrupadoCompanyDayHourArea = viajesdf.groupBy("Company", "Trip_Start_Date", "Trip_Start_Hour", "Pickup_Community_Area") \
    .agg(F.sum("Fare").alias("TotalFare"),
         F.sum("Tips").alias("TotalTips"),
         F.sum("Tolls").alias("TotalTolls"),
         F.sum("Extras").alias("TotalExtras"),
         F.sum("Trip_Total").alias("TotalTripTotal"),
         F.count("Trip_ID").alias("Trips"),
         F.countDistinct("Taxi_ID").alias("Taxis")) \
    .orderBy("Trips", ascending=False)

#    .agg({"Fare": "sum", "Tips": "sum", "Tolls": "sum", "Extras": "sum", "Trip_Total": "sum", "Trip_ID": "count", "Taxi_ID": "countDistinct"})\
#    .orderBy("count(Trip_ID)", ascending=False)

print agrupadoCompanyDayHourArea.count()
agrupadoCompanyDayHourArea.show(10)

#agrupado.write.jdbc
