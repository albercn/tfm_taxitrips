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

viajes = sparkSession.read.csv(path="/home/albercn/PycharmProjects/TFM_TaxiTrips/data_source/2017/", header=True, schema=schemaTaxiTrips, timestampFormat="MM/dd/yyyy hh:mm:ss a", mode="DROPMALFORMED")


viajesdf = viajes.filter(viajes.Company.isNotNull() & viajes.Pickup_Community_Area.isNotNull())\
    .select("Trip_ID", "Taxi_ID", "Company",
    F.to_date(F.date_format("Trip_Start_Timestamp", "yyyy-MM-dd")).alias("Trip_Start_Date"),
    F.hour("Trip_Start_Timestamp").alias("Trip_Start_Hour"),
    "Trip_Seconds", "Trip_Miles",
    "Pickup_Census_Tract", "Dropoff_Census_Tract", "Pickup_Community_Area", "Dropoff_Community_Area",
    F.regexp_replace(viajes["Fare"], '[\$,)]', '').astype('double').alias("Fare"),
    F.regexp_replace(viajes["Tips"], '[\$,)]', '').astype('double').alias("Tips"),
    F.regexp_replace(viajes["Tolls"], '[\$,)]', '').astype('double').alias("Tolls"),
    F.regexp_replace(viajes["Extras"], '[\$,)]', '').astype('double').alias("Extras"),
    F.regexp_replace(viajes["Trip_Total"], '[\$,)]', '').astype('double').alias("Trip_Total"),
    "Payment Type", "Pickup_Centroid_Latitude",
    "Pickup_Centroid_Longitude", "Pickup_Centroid_Location", "Dropoff_Centroid_Latitude", "Dropoff_Centroid_Longitude",
    "Dropoff_Centroid_Location")


# Agrupación por fecha, hora, empresa y zona
agrupadoCompanyDayHourArea = viajesdf.groupBy("Trip_Start_Date", "Trip_Start_Hour", "Company", "Pickup_Community_Area")\
    .agg(F.sum("Fare").alias("TotalFare"),
         F.sum("Tips").alias("TotalTips"),
         F.sum("Tolls").alias("TotalTolls"),
         F.sum("Extras").alias("TotalExtras"),
         F.sum("Trip_Total").alias("TotalTripTotal"),
         F.count("Trip_ID").alias("Trips"),
         F.countDistinct("Taxi_ID").alias("Taxis"))

# Escritura en BBDD
agrupadoCompanyDayHourArea.write.jdbc(url='jdbc:postgresql://localhost:5432/mydb', table='companies_zones_view', mode='overwrite', properties={'user': 'albercn', 'password': 'albercn'})

# Agrupación por fecha, hora y zona
agrupadoDayHourArea = viajesdf.groupBy("Trip_Start_Date", "Trip_Start_Hour", "Pickup_Community_Area")\
       .agg(F.sum("Fare").alias("TotalFare"),
         F.sum("Tips").alias("TotalTips"),
         F.sum("Tolls").alias("TotalTolls"),
         F.sum("Extras").alias("TotalExtras"),
         F.sum("Trip_Total").alias("TotalTripTotal"),
         F.count("Trip_ID").alias("Trips"),
         F.countDistinct("Taxi_ID").alias("Taxis"))
# Escritura en BBDD
agrupadoDayHourArea.write.jdbc(url='jdbc:postgresql://localhost:5432/mydb', table='zones_view', mode='overwrite', properties={'user': 'albercn', 'password': 'albercn'})


# Agrupación por fecha, taxi, empresa y zona
agrupadoDayTaxiCompanyArea = viajesdf.groupBy("Trip_Start_Date", "Taxi_ID", "Company", "Pickup_Community_Area")\
       .agg(F.sum("Fare").alias("TotalFare"),
         F.sum("Tips").alias("TotalTips"),
         F.sum("Tolls").alias("TotalTolls"),
         F.sum("Extras").alias("TotalExtras"),
         F.sum("Trip_Total").alias("TotalTripTotal"),
         F.count("Trip_ID").alias("Trips"))

# Escritura en BBDD
agrupadoDayTaxiCompanyArea.write.jdbc(url='jdbc:postgresql://localhost:5432/mydb', table='taxis_zones_day_view', mode='overwrite', properties={'user': 'albercn', 'password': 'albercn'})


# Agrupación por fecha, hora, taxi y empresa
agrupadoDayHourTaxiCompany = viajesdf.groupBy("Trip_Start_Date", "Trip_Start_Hour", "Taxi_ID", "Company")\
       .agg(F.sum("Fare").alias("TotalFare"),
         F.sum("Tips").alias("TotalTips"),
         F.sum("Tolls").alias("TotalTolls"),
         F.sum("Extras").alias("TotalExtras"),
         F.sum("Trip_Total").alias("TotalTripTotal"),
         F.count("Trip_ID").alias("Trips"))

# Escritura en BBDD
agrupadoDayHourTaxiCompany.write.jdbc(url='jdbc:postgresql://localhost:5432/mydb', table='taxis_view', mode='overwrite', properties={'user': 'albercn', 'password': 'albercn'})




