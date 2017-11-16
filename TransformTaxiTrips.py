#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

"""
Proceso para la transformación, enriquecimiento y agrupación de los viajes en taxi almacenados en S3 (AWS) o HDFS (local).
"""

from __future__ import print_function

# Importación del fichero de configuración
import taxi_trips_config as cfg

import sys
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructField, StructType
from pyspark.sql import functions as F


# Validación del número de parametros de entrada introducidos
if __name__ == "__main__":
    if len(sys.argv) != 2:
        yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
        tripsYear = str(yesterday.year)
    else:
        # Obtenemos año introducido como parámetro
        tripsYear = sys.argv[1]

# Creación de la SparkSession
sparkSession = SparkSession\
        .builder \
        .appName("Taxi-trips-batch")\
        .getOrCreate()

# Lectura de los datos de los viajes de taxi

# Construcción de la ruta de lectura de HDFS
pathTaxiTripsHDFS = cfg.trips_path + "year=" + tripsYear + "/"
# lectura
taxiTrips = sparkSession.read.parquet(pathTaxiTripsHDFS)\
    .distinct()\
    .select(
        "trip_id",
        "taxi_id",
        "company",
        F.to_timestamp(F.date_format("trip_start_timestamp", "yyyy-MM-dd 00:00:00")).alias("trip_start_date"),
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

# Lectura del los datos de los areas
# Creación el esquema del dataframe
schemaAreas = StructType([
    StructField("area_number", IntegerType(), False),
    StructField("community", StringType(), False),
    StructField("area_centroid_latitude", StringType(), True),
    StructField("area_centroid_longitude", StringType(), True),
    StructField("the_geom", StringType(), True)
])

# Lectura del fichero
areas = sparkSession.read.csv(path=cfg.area_path,
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

# Cruce del dataframe TaxiTrips con los nombres de los areas de inicio y fin, para enriquecerlo
# con la localización y el nombre lode areas de inicio y fin
taxiTripsEnrich = taxiTrips.join(pickupAreas, 'pickup_community_area', 'left')\
    .join(dropoffAreas, 'dropoff_community_area', 'left')


# Agrupación por fecha, empresa y area de inicio
groupByCompanyDayHourArea = taxiTripsEnrich\
    .groupBy("trip_start_date",
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
# Escritura en BBDD
groupByCompanyDayHourArea.write.jdbc(url=cfg.jbdc_url,
                                     table='companies_pickup_area_view_' + tripsYear,
                                     mode='overwrite',
                                     properties=cfg.jdbc_user
                                     )


# Agrupación por fecha y area de inicio
groupByDayHourArea = groupByCompanyDayHourArea\
    .groupBy("trip_start_date",
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
groupByDayHourArea.write.jdbc(url=cfg.jbdc_url,
                              table='pickup_area_view_' + tripsYear,
                              mode='overwrite',
                              properties=cfg.jdbc_user
                              )

# AREAS DE FIN

# Agrupación por fecha, empresa y area de fin
groupByCompanyDayHourDropoffArea = taxiTripsEnrich\
    .groupBy("trip_start_date",
             "company",
             "dropoff_community_area",
             "dropoff_community_area_name",
             "dropoff_centroid_latitude",
             "dropoff_centroid_longitude"
             )\
    .agg(F.sum("fare").alias("fares"),
         F.sum("tips").alias("tips"),
         F.sum("tolls").alias("tolls"),
         F.sum("extras").alias("extras"),
         F.sum("trip_total").alias("trip_totals"),
         F.count("trip_id").alias("trips"),
         F.countDistinct("taxi_id").alias("taxis")
         )

# Escritura en BBDD
groupByCompanyDayHourDropoffArea.write.jdbc(url=cfg.jbdc_url,
                                             table='companies_dropoff_area_view_' + tripsYear,
                                             mode='overwrite',
                                             properties=cfg.jdbc_user
                                            )

# Agrupación por fecha y area de fin
groupByDayHourDropoffArea = groupByCompanyDayHourDropoffArea\
    .groupBy("trip_start_date",
             "dropoff_community_area",
             "dropoff_community_area_name",
             "dropoff_centroid_latitude",
             "dropoff_centroid_longitude")\
    .agg(F.sum("fares").alias("fares"),
         F.sum("tips").alias("tips"),
         F.sum("tolls").alias("tolls"),
         F.sum("extras").alias("extras"),
         F.sum("trip_totals").alias("trip_totals"),
         F.sum("trips").alias("trips"),
         F.countDistinct("taxis").alias("taxis")
         )

# Escritura en BBDD
groupByDayHourDropoffArea.write.jdbc(url=cfg.jbdc_url,
                              table='dropoff_area_view_' + tripsYear,
                              mode='overwrite',
                              properties=cfg.jdbc_user
                               )






