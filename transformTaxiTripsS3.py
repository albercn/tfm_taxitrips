#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

from __future__ import print_function

import sys
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StringType, IntegerType, StructField, StructType
from pyspark.sql import functions as F


# Validación del número de parametros de entrada introducidos
if __name__ == "__main__":
    if len(sys.argv) != 2:
        yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
        tripsYear = str(yesterday.year)
    else:
        # Obtenemos año introducido como parametro
        tripsYear = sys.argv[1]

sparkSession = SparkSession\
        .builder \
        .appName("Taxi-trips-batch")\
        .getOrCreate()

"""
OPCION HIVE

sparkSession = SparkSession\
        .builder \
        .appName("Taxi-trips-batch")\
        .config("spark.sql.warehouse.dir", '/home/albercn/opt/hive/warehouse')\
        .enableHiveSupport()\
        .getOrCreate()

"""

"""
Lectura de los datos de los viajes de taxi de hdfs
"""

try:
        # Construcción de la ruta de lectura de HDFS
        pathTaxiTripsHDFS = "s3a://taxi-trips-tfm/trips-events/year=" + tripsYear + "/"
        # lectura
        taxiTrips = sparkSession.read.parquet(pathTaxiTripsHDFS)\
            .distinct()\
            .select(
                "trip_id",
                "taxi_id",
                "company",
                F.to_timestamp(F.date_format("trip_start_timestamp", "yyyy-MM-dd HH:00:00")).alias("trip_start_date_hour"),
                F.to_date(F.date_format("trip_start_timestamp", "yyyy-MM-dd")).alias("trip_start_date"),
                "trip_seconds",
                "trip_miles",
                "pickup_community_area",
                "dropoff_community_area",
                "fare",
                "tips",
                "tolls",
                "extras",
                "trip_total",
                "payment_type"
            )

except(AnalysisException):
        print("No existen datos del año " + tripsYear, file=sys.stderr)
        exit(-1)

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
areas = sparkSession.read.csv(path="s3a://taxi-trips-tfm/areas/",
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
taxiTripsEnrich = taxiTrips.join(pickupAreas, 'pickup_community_area', 'left')\
    .join(dropoffAreas, 'dropoff_community_area', 'left')


# Agrupación por fecha, hora, empresa y area de inicio
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
"""
OPCIÓN HIVE

groupByCompanyDayHourArea.write.saveAsTable('companies_pickup_area_view_' + tripsYear, mode='overwrite')
"""
# Escritura en BBDD
groupByCompanyDayHourArea.write.jdbc(url='jdbc:postgresql://taxi-trips.cdihiorubekt.eu-west-1.rds.amazonaws.com:5432/taxitrips',
                                     table='companies_pickup_area_view_' + tripsYear,
                                     mode='overwrite',
                                     properties={'user': 'albercn', 'password': 'K$chool_TFM'}
                                     )


# Agrupación por fecha, hora y area de inicio
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
         F.sum("trips").alias("trips"),
         F.countDistinct("taxis").alias("taxis")
         )


# Escritura en BBDD
groupByDayHourArea.write.jdbc(url='jdbc:postgresql://taxi-trips.cdihiorubekt.eu-west-1.rds.amazonaws.com:5432/taxitrips',
                              table='pickup_area_view_' + tripsYear,
                              mode='overwrite',
                              properties={'user': 'albercn', 'password': 'K$chool_TFM'})

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
groupByDayTaxiCompanyArea.write.jdbc(url='jdbc:postgresql://taxi-trips.cdihiorubekt.eu-west-1.rds.amazonaws.com:5432/taxitrips',
                                     table='taxi_pickup_area_day_view_' + tripsYear,
                                     mode='overwrite',
                                     properties={'user': 'albercn', 'password': 'K$chool_TFM'})

