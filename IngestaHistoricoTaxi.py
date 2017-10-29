#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, IntegerType, StructField, StructType, DoubleType

# Validación del número de parametros de entrada introducidos
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: IngestaHistorico.py <year>", file=sys.stderr)
        exit(-1)

# Obtenemos la url del broker y el topic de entrada
year = sys.argv[1]

sparkSession = SparkSession\
                .builder\
                .master("local[2]")\
                .appName("IngestaHistoricoTaxiTrips") \
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
    StructField("Payment_Type", StringType(), True),
    StructField("Company", StringType(), True),
    StructField("Pickup_Centroid_Latitude", DoubleType(), True),
    StructField("Pickup_Centroid_Longitude", DoubleType(), True),
    StructField("Pickup_Centroid_Location", StringType(), True),
    StructField("Dropoff_Centroid_Latitude", DoubleType(), True),
    StructField("Dropoff_Centroid_Longitude", DoubleType(), True),
    StructField("Dropoff_Centroid_Location", StringType(), True)
])

filePath = "file:///home/albercn/PycharmProjects/TFM_TaxiTrips/data_source/" + year + "/"

taxiTrips = sparkSession.read.csv(path=filePath, header=True, schema=schemaTaxiTrips,
                                  timestampFormat="MM/dd/yyyy hh:mm:ss a", mode="DROPMALFORMED")

taxiTripsdf = taxiTrips.filter(taxiTrips.Company.isNotNull() & taxiTrips.Pickup_Community_Area.isNotNull())\
    .select("Trip_ID", "Taxi_ID", "Company",
    F.to_timestamp(F.date_format("Trip_Start_Timestamp", "yyyy-MM-dd hh:00:00").cast('string')).alias("Trip_Start_Date_Hour"),
    F.to_date(F.date_format("Trip_Start_Timestamp", "yyyy-MM-dd")).alias("Trip_Start_Date"),
    "Trip_Seconds", "Trip_Miles",
    "Pickup_Census_Tract", "Dropoff_Census_Tract", "Pickup_Community_Area", "Dropoff_Community_Area",
    F.regexp_replace(taxiTrips["Fare"], '[\$,)]', '').astype('double').alias("Fare"),
    F.regexp_replace(taxiTrips["Tips"], '[\$,)]', '').astype('double').alias("Tips"),
    F.regexp_replace(taxiTrips["Tolls"], '[\$,)]', '').astype('double').alias("Tolls"),
    F.regexp_replace(taxiTrips["Extras"], '[\$,)]', '').astype('double').alias("Extras"),
    F.regexp_replace(taxiTrips["Trip_Total"], '[\$,)]', '').astype('double').alias("Trip_Total"),
    "Payment_Type")


# Lectura del fichero con los areas
# Creamos el esquema del dataframe
schemaAreas = StructType([
    StructField("Area_Number", IntegerType(), False),
    StructField("Community", StringType(), False),
    StructField("Area_Centroid_Latitude", StringType(), True),
    StructField("Area_Centroid_Longitude", StringType(), True),
    StructField("The_Geom", StringType(), True)
])

# Lectura del fichero la información de los areas
areas = sparkSession.read.csv(path="hdfs://localhost:9000/TaxiTrips/areas/", header=True, schema=schemaAreas, mode="DROPMALFORMED")

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


# Cruce del dataframe TaxiTrips con los nombres de los areas de inicio y fin
taxiTripsEnrich = taxiTripsdf.join(pickupAreas, 'Pickup_Community_Area')\
    .join(dropoffAreas, 'Dropoff_Community_Area')


# Agrupación por fecha, hora, empresa y zona
groupByCompanyDayHourArea = taxiTripsEnrich.groupBy("Trip_Start_Date_Hour", "Company",
                                                "Pickup_Community_Area", "Pickup_Community_Area_Name",
                                                "Pickup_Centroid_Latitude", "Pickup_Centroid_Longitude")\
    .agg(F.sum("Fare").alias("TotalFare"),
         F.sum("Tips").alias("TotalTips"),
         F.sum("Tolls").alias("TotalTolls"),
         F.sum("Extras").alias("TotalExtras"),
         F.sum("Trip_Total").alias("TotalTripTotal"),
         F.count("Trip_ID").alias("Trips"),
         F.countDistinct("Taxi_ID").alias("Taxis"))

# Escritura en BBDD
groupByCompanyDayHourArea.write.jdbc(url='jdbc:postgresql://localhost:5432/mydb', table='companies_pickup_area_view',
                                     mode='append', properties={'user': 'albercn', 'password': 'albercn'})

# Agrupación por fecha, hora y zona
groupByDayHourArea = taxiTripsEnrich.groupBy("Trip_Start_Date_Hour", "Pickup_Community_Area",
                                         "Pickup_Community_Area_Name", "Pickup_Centroid_Latitude",
                                         "Pickup_Centroid_Longitude")\
       .agg(F.sum("Fare").alias("TotalFare"),
            F.sum("Tips").alias("TotalTips"),
            F.sum("Tolls").alias("TotalTolls"),
            F.sum("Extras").alias("TotalExtras"),
            F.sum("Trip_Total").alias("TotalTripTotal"),
            F.count("Trip_ID").alias("Trips"),
            F.countDistinct("Taxi_ID").alias("Taxis"))
# Escritura en BBDD
groupByDayHourArea.write.jdbc(url='jdbc:postgresql://localhost:5432/mydb', table='pickup_area_view', mode='append',
                              properties={'user': 'albercn', 'password': 'albercn'})


# Agrupación por fecha, taxi, empresa y zona
groupByDayTaxiCompanyArea = taxiTripsEnrich.groupBy("Trip_Start_Date", "Taxi_ID", "Company", "Pickup_Community_Area",
                                                "Pickup_Community_Area_Name", "Pickup_Centroid_Latitude",
                                                "Pickup_Centroid_Longitude")\
       .agg(F.sum("Fare").alias("TotalFare"),
            F.sum("Tips").alias("TotalTips"),
            F.sum("Tolls").alias("TotalTolls"),
            F.sum("Extras").alias("TotalExtras"),
            F.sum("Trip_Total").alias("TotalTripTotal"),
            F.count("Trip_ID").alias("Trips"))

# Escritura en BBDD
groupByDayTaxiCompanyArea.write.jdbc(url='jdbc:postgresql://localhost:5432/mydb', table='taxi_pickup_area_day_view',
                                     mode='append', properties={'user': 'albercn', 'password': 'albercn'})

# Agrupación por fecha, hora, taxi y empresa
groupByDayHourTaxiCompany = taxiTripsEnrich.groupBy("Trip_Start_Date_Hour", "Taxi_ID", "Company")\
       .agg(F.sum("Fare").alias("TotalFare"),
            F.sum("Tips").alias("TotalTips"),
            F.sum("Tolls").alias("TotalTolls"),
            F.sum("Extras").alias("TotalExtras"),
            F.sum("Trip_Total").alias("TotalTripTotal"),
            F.count("Trip_ID").alias("Trips"))

# Escritura en BBDD
groupByDayHourTaxiCompany.write.jdbc(url='jdbc:postgresql://localhost:5432/mydb', table='taxis_view', mode='append', 
                                     properties={'user': 'albercn', 'password': 'albercn'})


