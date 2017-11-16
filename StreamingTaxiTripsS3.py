#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

# Importación del fichero de configuración
import taxi_trips_config as cfg

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, TimestampType, IntegerType, StructField, StructType
from pyspark.sql.functions import from_json, regexp_replace, to_json, struct, year, month

# Creación de la SparkSession
sparkSession = SparkSession\
        .builder\
        .appName("taxi-trips-streaming")\
        .getOrCreate()

# Lectura del fichero con los areas que utilizaremos para enriquecer los datos de los viajes leídos de kafka
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

# Creación del dataframe para cruzar con TaxiTrips por pickup_community_area
pickupAreas = areas.select(
    areas["area_Number"].alias('pickup_community_area'),
    areas["community"].alias('pickup_community_area_name'),
    areas["area_centroid_latitude"].alias('pickup_centroid_latitude'),
    areas["area_centroid_longitude"].alias('pickup_centroid_longitude')
)
# Creación del dataframe para cruzar con TaxiTrips por dropoff_community_area
dropoffAreas = areas.select(
    areas["area_Number"].alias('dropoff_community_area'),
    areas["community"].alias('dropoff_community_area_name'),
    areas["area_centroid_latitude"].alias('dropoff_centroid_latitude'),
    areas["area_centroid_longitude"].alias('dropoff_centroid_longitude')
)

# Obtención de kafka de los mensajes con la información de los viajes
# Definición del esquema del json
schemaJsonTaxiTrips = StructType()\
    .add("payment_type", StringType())\
    .add("dropoff_census_tract", StringType())\
    .add("tolls", StringType())\
    .add("trip_total", StringType())\
    .add("dropoff_centroid_latitude", StringType())\
    .add("fare", StringType())\
    .add("tips", StringType())\
    .add("pickup_census_tract", StringType())\
    .add("company", StringType())\
    .add("trip_start_timestamp", TimestampType())\
    .add("trip_miles", StringType())\
    .add("dropoff_community_area", StringType())\
    .add("taxi_id", StringType())\
    .add("trip_id", StringType())\
    .add("pickup_centroid_latitude", StringType())\
    .add("extras", StringType())\
    .add("dropoff_centroid_location", StringType())\
    .add("trip_seconds", StringType())\
    .add("pickup_centroid_location", StringType())\
    .add("trip_end_timestamp", TimestampType())\
    .add("pickup_community_area", StringType())\
    .add("dropoff_centroid_longitude", StringType())\
    .add("pickup_centroid_longitude", StringType())

# Formato del timestamp
tripTimestampFormat = "MM/dd/yyyy hh:mm:ss a"
jsonOptions = {"timestampFormat": tripTimestampFormat}

# Creación del DataFrame que representa el stream de viajes en taxi
kst = sparkSession\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", cfg.kafka_brokers)\
        .option("subscribe", cfg.kafka_inTopic)\
        .option("failOnDataLoss", False)\
        .load()\
        .selectExpr("CAST(value AS STRING)")

# Parseo del mensaje
parsed = kst.select(from_json(kst.value, schemaJsonTaxiTrips, jsonOptions).alias("parsed_value"))
# Obtención de los campos incluidos en el mensaje
taxiTripsRaw = parsed.select("parsed_value.*")\

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
    regexp_replace(taxiTripsRaw["fare"], '[\$,)]', '').astype('double').alias("fare"),
    regexp_replace(taxiTripsRaw["tips"], '[\$,)]', '').astype('double').alias("tips"),
    regexp_replace(taxiTripsRaw["tolls"], '[\$,)]', '').astype('double').alias("tolls"),
    regexp_replace(taxiTripsRaw["extras"], '[\$,)]', '').astype('double').alias("extras"),
    regexp_replace(taxiTripsRaw["trip_total"], '[\$,)]', '').astype('double').alias("trip_total"),
    "payment_type",
    "company",
    "pickup_centroid_latitude",
    "pickup_centroid_longitude",
    "pickup_centroid_location",
    "dropoff_centroid_latitude",
    "dropoff_centroid_longitude",
    "dropoff_centroid_location",
    year(taxiTripsRaw["trip_start_timestamp"]).alias("year"),
    month(taxiTripsRaw["trip_start_timestamp"]).alias("month")
)

# Selección los campos que se enviarán a Druid través de kafka
taxiTripsToKafka = taxiTrips.select("trip_id",
            "taxi_id",
            "company",
            "trip_start_timestamp",
            "trip_end_timestamp",
            "trip_seconds",
            "trip_miles",
            "pickup_community_area",
            "dropoff_community_area",
            "fare",
            "tips",
            "tolls",
            "extras",
            "trip_total"
            )

# Enriquecimiento del Stream con los nombres de los areas de inicio y fin, y sus puntos centrales(lat. y long.)
taxiTripsEnrich = taxiTripsToKafka.join(pickupAreas, 'pickup_community_area')\
    .join(dropoffAreas, 'dropoff_community_area')

# Inicio de la query que escribe el resultado del enriquecimiennto a kafka
queryToKafka = taxiTripsEnrich\
    .select(taxiTripsEnrich["taxi_id"].cast('string').alias("key"),
            to_json(struct("*")).alias("value"))\
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", cfg.kafka_brokers) \
    .option("topic", cfg.kafka_outTopic) \
    .option("checkpointLocation", cfg.checkpointKafka_path) \
    .outputMode("Append") \
    .start()

# Inicio de la query que escribe los eventos a HDFS
queryToHDFS = taxiTrips.writeStream \
        .format("parquet") \
        .trigger(processingTime='15 minutes') \
        .partitionBy("year", "month") \
        .option("path", cfg.trips_path) \
        .option("checkpointLocation", cfg.checkpointHDFS_path) \
        .outputMode("Append") \
        .start()

queryToKafka.awaitTermination()
queryToHDFS.awaitTermination()


