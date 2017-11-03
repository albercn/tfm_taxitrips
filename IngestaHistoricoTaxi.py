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
    StructField("trip_id", StringType(), False),
    StructField("taxi_id", StringType(), False),
    StructField("trip_start_timestamp", TimestampType(), False),
    StructField("trip_end_timestamp", TimestampType(), False),
    StructField("trip_seconds", IntegerType(), True),
    StructField("trip_miles", DoubleType(), True),
    StructField("pickup_census_tract", StringType(), True),
    StructField("dropoff_census_tract", StringType(), True),
    StructField("pickup_community_area", IntegerType(), True),
    StructField("dropoff_community_area", IntegerType(), True),
    StructField("fare", StringType(), True),
    StructField("tips", StringType(), True),
    StructField("tolls", StringType(), True),
    StructField("extras", StringType(), True),
    StructField("trip_total", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("company", StringType(), True),
    StructField("pickup_centroid_latitude", DoubleType(), True),
    StructField("pickup_centroid_longitude", DoubleType(), True),
    StructField("pickup_centroid_location", StringType(), True),
    StructField("dropoff_centroid_latitude", DoubleType(), True),
    StructField("dropoff_centroid_longitude", DoubleType(), True),
    StructField("dropoff_centroid_location", StringType(), True)
])

filePath = "file:///home/albercn/PycharmProjects/TFM_TaxiTrips/data_source/" + year + "/"

taxiTrips = sparkSession.read.csv(path=filePath, header=True, schema=schemaTaxiTrips,
                                  timestampFormat="MM/dd/yyyy hh:mm:ss a", mode="DROPMALFORMED")

taxiTripsFormat = taxiTrips.filter((F.year("trip_start_timestamp").astype('string') == year) & taxiTrips.company.isNotNull() & taxiTrips.pickup_community_area.isNotNull())\
    .select("trip_id", "taxi_id", "company",
    F.to_timestamp(F.date_format("trip_start_timestamp", "yyyy-MM-dd hh:00:00").cast('string')).alias("trip_start_date_hour"),
    F.to_date(F.date_format("trip_start_timestamp", "yyyy-MM-dd")).alias("trip_start_date"),
    "trip_seconds", "trip_miles",
    "pickup_census_tract", "dropoff_census_tract", "pickup_community_area", "dropoff_community_area",
    F.regexp_replace(taxiTrips["fare"], '[\$,)]', '').astype('double').alias("fare"),
    F.regexp_replace(taxiTrips["tips"], '[\$,)]', '').astype('double').alias("tips"),
    F.regexp_replace(taxiTrips["tolls"], '[\$,)]', '').astype('double').alias("tolls"),
    F.regexp_replace(taxiTrips["extras"], '[\$,)]', '').astype('double').alias("extras"),
    F.regexp_replace(taxiTrips["trip_total"], '[\$,)]', '').astype('double').alias("trip_total"),
    "payment_type")


taxiTripsdf = taxiTripsFormat.filter(taxiTripsFormat.trip_start_date_hour.isNotNull())


# Lectura del fichero con los areas
# Creamos el esquema del dataframe
schemaAreas = StructType([
    StructField("area_number", IntegerType(), False),
    StructField("community", StringType(), False),
    StructField("area_centroid_latitude", StringType(), True),
    StructField("area_centroid_longitude", StringType(), True),
    StructField("the_geom", StringType(), True)
])

# Lectura del fichero la información de los areas
areas = sparkSession.read.csv(path="hdfs://localhost:9000/TaxiTrips/areas/", header=True, schema=schemaAreas, mode="DROPMALFORMED")

# Creación del dataframe para cruzar con TaxiTrips por pickup_community_area
pickupAreas = areas.select(
    areas["area_number"].alias('pickup_community_area'),
    areas["community"].alias('pickup_community_area_name'),
    areas["area_centroid_latitude"].alias('pickup_centroid_latitude'),
    areas["area_centroid_longitude"].alias('pickup_centroid_longitude')
)
# Creación del dataframe para cruzar con TaxiTrips por dropoff_community_area
dropoffAreas = areas.select(
    areas["area_number"].alias('dropoff_community_area'),
    areas["community"].alias('dropoff_community_area_name'),
    areas["area_centroid_latitude"].alias('dropoff_centroid_latitude'),
    areas["area_centroid_longitude"].alias('dropoff_centroid_longitude')
)


# Cruce del dataframe TaxiTrips con los nombres de los areas de inicio y fin
taxiTripsEnrich = taxiTripsdf.join(pickupAreas, 'pickup_community_area')\
    .join(dropoffAreas, 'dropoff_community_area')


# Agrupación por fecha, hora, empresa y zona
groupByCompanyDayHourArea = taxiTripsEnrich.groupBy("trip_start_date", "trip_start_date_hour", "company",
                                                "pickup_community_area", "pickup_community_area_name",
                                                "pickup_centroid_latitude", "pickup_centroid_longitude")\
    .agg(F.sum("fare").alias("fares"),
         F.sum("tips").alias("tips"),
         F.sum("tolls").alias("tolls"),
         F.sum("extras").alias("extras"),
         F.sum("trip_total").alias("trip_totals"),
         F.count("trip_id").alias("trips"),
         F.countDistinct("taxi_id").alias("taxis"))

# Escritura en BBDD
groupByCompanyDayHourArea.write.jdbc(url='jdbc:postgresql://localhost:5432/mydb', table='companies_pickup_area_view',
                                     mode='append', properties={'user': 'albercn', 'password': 'albercn'})

# Agrupación por fecha, hora y zona
groupByDayHourArea = taxiTripsEnrich.groupBy("trip_start_date", "trip_start_date_hour", "pickup_community_area",
                                         "pickup_community_area_name", "pickup_centroid_latitude",
                                         "pickup_centroid_longitude")\
       .agg(F.sum("fare").alias("fares"),
            F.sum("tips").alias("tips"),
            F.sum("tolls").alias("tolls"),
            F.sum("extras").alias("extras"),
            F.sum("trip_total").alias("trip_totals"),
            F.count("trip_id").alias("trips"),
            F.countDistinct("taxi_id").alias("taxis"))
# Escritura en BBDD
groupByDayHourArea.write.jdbc(url='jdbc:postgresql://localhost:5432/mydb', table='pickup_area_view', mode='append',
                              properties={'user': 'albercn', 'password': 'albercn'})


# Agrupación por fecha, taxi, empresa y zona
groupByDayTaxiCompanyArea = taxiTripsEnrich.groupBy("trip_start_date", "taxi_id", "company", "pickup_community_area",
                                                "pickup_community_area_name", "pickup_centroid_latitude",
                                                "pickup_centroid_longitude")\
       .agg(F.sum("fare").alias("fares"),
            F.sum("tips").alias("tips"),
            F.sum("tolls").alias("tolls"),
            F.sum("extras").alias("extras"),
            F.sum("trip_total").alias("trip_totals"),
            F.count("trip_id").alias("trips"))

# Escritura en BBDD
groupByDayTaxiCompanyArea.write.jdbc(url='jdbc:postgresql://localhost:5432/mydb', table='taxi_pickup_area_day_view',
                                     mode='append', properties={'user': 'albercn', 'password': 'albercn'})

# Agrupación por fecha, hora, taxi y empresa
groupByDayHourTaxiCompany = taxiTripsEnrich.groupBy("trip_start_date", "trip_start_date_hour", "taxi_id", "company")\
       .agg(F.sum("fare").alias("fares"),
            F.sum("tips").alias("tips"),
            F.sum("tolls").alias("tolls"),
            F.sum("extras").alias("extras"),
            F.sum("trip_total").alias("trip_totals"),
            F.count("trip_id").alias("trips"))

# Escritura en BBDD
groupByDayHourTaxiCompany.write.jdbc(url='jdbc:postgresql://localhost:5432/mydb', table='taxis_view', mode='append', 
                                     properties={'user': 'albercn', 'password': 'albercn'})


