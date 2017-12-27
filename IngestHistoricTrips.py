#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

"""
Proceso para la ingesta de los ficheros de históricos en en S3 (AWS) o HDFS (local) en formato parquet,
para ser transformados posteriormente.
"""

from __future__ import print_function

# Importación del fichero de configuración
import taxi_trips_config_local as cfg

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, IntegerType, StructField, StructType, DoubleType

# Validación del número de parametros de entrada introducidos
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: IngestaHistorico.py <year>", file=sys.stderr)
        exit(-1)

# Obtenemos el año de los parametros de entrada
year = sys.argv[1]

# Creación de la SparkSession
sparkSession = SparkSession\
                .builder\
                .appName("IngestaHistoricoTaxiTrips") \
                .getOrCreate()


# Lectura del fichero con el histórico
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
    StructField("pickup_centroid_latitude", StringType(), True),
    StructField("pickup_centroid_longitude", StringType(), True),
    StructField("pickup_centroid_location", StringType(), True),
    StructField("dropoff_centroid_latitude", StringType(), True),
    StructField("dropoff_centroid_longitude", StringType(), True),
    StructField("dropoff_centroid_location", StringType(), True)
])

# Lectura de los datos de los viajes en taxi
filePath = cfg.csv_input_path + year + "/"
taxiTripsRaw = sparkSession.read.csv(path=filePath, header=True,
                                     schema=schemaTaxiTrips,
                                     timestampFormat="MM/dd/yyyy hh:mm:ss a",
                                     mode="DROPMALFORMED")
# Limpieza de los datos
taxiTrips = taxiTripsRaw.select(
    "trip_id",
    "taxi_id",
    "trip_start_timestamp",
    "trip_end_timestamp",
    taxiTripsRaw["trip_seconds"].astype('integer').alias("trip_seconds"),
    taxiTripsRaw["trip_miles"].astype('integer').alias("trip_miles"),
    "pickup_census_tract",
    "dropoff_census_tract",
    taxiTripsRaw["pickup_community_area"].astype('integer').alias("pickup_community_area"),
    taxiTripsRaw["dropoff_community_area"].astype('integer').alias("dropoff_community_area"),
    F.regexp_replace(taxiTripsRaw["fare"], '[\$,)]', '').astype('double').alias("fare"),
    F.regexp_replace(taxiTripsRaw["tips"], '[\$,)]', '').astype('double').alias("tips"),
    F.regexp_replace(taxiTripsRaw["tolls"], '[\$,)]', '').astype('double').alias("tolls"),
    F.regexp_replace(taxiTripsRaw["extras"], '[\$,)]', '').astype('double').alias("extras"),
    F.regexp_replace(taxiTripsRaw["trip_total"], '[\$,)]', '').astype('double').alias("trip_total"),
    "payment_type",
    "company",
    "pickup_centroid_latitude",
    "pickup_centroid_longitude",
    "pickup_centroid_location",
    "dropoff_centroid_latitude",
    "dropoff_centroid_longitude",
    "dropoff_centroid_location",
    F.year(taxiTripsRaw["trip_start_timestamp"]).alias("year"),
    F.month(taxiTripsRaw["trip_start_timestamp"]).alias("month")
)

# Escritura de los datos transformados en S3 (AWS) o HDFS (local), particionados por año y mes
taxiTrips.write.parquet(path=cfg.trips_path,
                        mode="append",
                        partitionBy=["year", "month"])

