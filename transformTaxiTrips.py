#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

from __future__ import print_function

import sys
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StringType, TimestampType, IntegerType, StructField, StructType, DoubleType
from pyspark.sql.functions import year, regexp_replace, to_timestamp, to_date, date_format
from pyspark.sql import functions as F


# Validación del número de parametros de entrada introducidos
if __name__ == "__main__":
    if len(sys.argv) != 2:
        yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
        year = str(yesterday.year)
    else:
        # Obtenemos año introducido como parametro
        year = sys.argv[1]

sparkSession = SparkSession\
        .builder \
        .master("local[2]") \
        .appName("Taxi-trips-batch")\
        .getOrCreate()

"""
Lectura de los datos de los viajes de taxi de hdfs
"""

parquet = True
try:
        # Construcción de la ruta de lectura de HDFS
        pathTaxiTripsHDFS = "hdfs://localhost:9000/TaxiTrips/rawEvents/year=" + str(year) + "/"
        # lectura
        taxiTrips = sparkSession.read.parquet(pathTaxiTripsHDFS)\
            .distinct()\
            .select(
                "trip_id",
                "taxi_id",
                "trip_start_timestamp",
                "trip_end_timestamp",
                "trip_seconds",
                "trip_miles",
                "pickup_census_tract",
                "dropoff_census_tract",
                "pickup_community_area",
                "dropoff_community_area",
                "fare",
                "tips",
                "tolls",
                "extras",
                "trip_total",
                "payment_type",
                "company",
                "pickup_centroid_latitude",
                "pickup_centroid_longitude",
                "pickup_centroid_location",
                "dropoff_centroid_latitude",
                "dropoff_centroid_longitude",
                "dropoff_centroid_location"
            )

except(AnalysisException):
        print("No existen datos del año " + str(year), file=sys.stderr)
        exit(-1)

taxiTripsFormat = taxiTrips\
    .filter(
        (F.year("trip_start_timestamp").astype('string') == year)
    )\
    .select(
        "trip_id",
        "taxi_id", "company",
        to_timestamp(date_format("trip_start_timestamp", "yyyy-MM-dd hh:00:00+01:00").cast('string')).alias("trip_start_date_hour"),
        to_date(date_format("trip_start_timestamp", "yyyy-MM-dd")).alias("trip_start_date"),
        "trip_seconds",
        "trip_miles",
        "pickup_census_tract",
        "dropoff_census_tract",
        "pickup_community_area",
        "dropoff_community_area",
        "fare",
        "tips",
        "tolls",
        "extras",
        "trip_total",
        "payment_type"
    )

taxiTripsdf = taxiTripsFormat.filter(taxiTripsFormat.trip_start_date_hour.isNotNull())


"""
Lectura del fichero con los areas
"""
# Creamos el esquema del dataframe
schemaAreas = StructType([
    StructField("area_number", IntegerType(), False),
    StructField("community", StringType(), False),
    StructField("area_centroid_latitude", StringType(), True),
    StructField("area_centroid_longitude", StringType(), True),
    StructField("the_geom", StringType(), True)
])

# Lectura del fichero
areas = sparkSession.read.csv(path="hdfs://localhost:9000/TaxiTrips/areas/",
                              header=True,
                              schema=schemaAreas,
                              mode="DROPMALFORMED")

# Creación del dataframe 'pickupAreas' para cruzar con TaxiTrips por pickup_community_area
pickupAreas = areas.select(
    areas["area_number"].alias('pickup_community_area'),
    areas["community"].alias('pickup_community_area_name'),
    areas["area_centroid_latitude"].alias('pickup_centroid_latitude'),
    areas["area_centroid_longitude"].alias('pickup_centroid_longitude')
)
# Creación del dataframe 'dropoffAreas' para cruzar con TaxiTrips por dropoff_community_area
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
groupByCompanyDayHourArea = taxiTripsEnrich\
    .groupBy("trip_start_date_hour",
             "company",
             "pickup_community_area",
             "pickup_community_area_name",
             "pickup_centroid_latitude",
             "pickup_centroid_longitude"
             )\
    .agg(F.sum("fare").alias("fares"),
         F.sum("tips").alias("tips"),
         F.sum("tolls").alias("tolls"),
         F.sum("extras").alias("extras"),
         F.sum("trip_total").alias("trip_totals"),
         F.count("trip_id").alias("trips"),
         F.countDistinct("taxi_id").alias("taxis")
         )
# Composición de la vista en la que se escribiran los datos
companies_pickup_area_view = 'companies_pickup_area_view_' + str(year)

# Escritura en BBDD
groupByCompanyDayHourArea.write.jdbc(url='jdbc:postgresql://localhost:5432/taxitrips',
                                     table=companies_pickup_area_view,
                                     mode='overwrite',
                                     properties={'user': 'albercn', 'password': 'albercn'})


# Agrupación por fecha, hora y zona
groupByDayHourArea = groupByCompanyDayHourArea\
    .groupBy("trip_start_date_hour",
             "pickup_community_area",
             "pickup_community_area_name",
             "pickup_centroid_latitude",
             "pickup_centroid_longitude")\
    .agg(F.sum("fares").alias("fares"),
         F.sum("tips").alias("tips"),
         F.sum("tolls").alias("tolls"),
         F.sum("extras").alias("extras"),
         F.sum("trip_totals").alias("trip_totals"),
         F.count("trips").alias("trips"),
         F.countDistinct("taxis").alias("taxis")
         )
# Escritura en BBDD
groupByDayHourArea.write.jdbc(url='jdbc:postgresql://localhost:5432/taxitrips',
                              table='pickup_area_view_' + str(year),
                              mode='overwrite',
                              properties={'user': 'albercn', 'password': 'albercn'})


# Agrupación por fecha, taxi, empresa y zona
groupByDayTaxiCompanyArea = taxiTripsEnrich\
    .groupBy("trip_start_date",
             "taxi_id",
             "company",
             "pickup_community_area",
             "pickup_community_area_name",
             "pickup_centroid_latitude",
             "pickup_centroid_longitude"
             )\
    .agg(F.sum("fare").alias("fares"),
         F.sum("tips").alias("tips"),
         F.sum("tolls").alias("tolls"),
         F.sum("extras").alias("extras"),
         F.sum("trip_total").alias("trip_totals"),
         F.count("trip_id").alias("trips")
         )
# Escritura en BBDD
groupByDayTaxiCompanyArea.write.jdbc(url='jdbc:postgresql://localhost:5432/taxitrips',
                                     table='taxi_pickup_area_day_view_' + str(year),
                                     mode='overwrite',
                                     properties={'user': 'albercn', 'password': 'albercn'})

