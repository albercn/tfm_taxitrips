# TFM Taxi Trips
Sistema para el procesamiento y análisis de viajes en taxi.

## Requisitos
Se quiere disponer un sistema con el que poder:
 
* Procesar, en tiempo real, la información de cada viaje enviada por los taxis.  
* Ejecutar de distintos tipos de consultas sobre la información en tiempo real. Por ejemplo, conocer el número de taxis activos total y por zonas o compañías, volumen total de viajes y  por zonas o compañías en las últimas horas.
* Analizar el histórico completo de datos, para poder ejecutar consultas y obtener distintas estadísticas de actividad de los taxis. Por ejemplo, duración y coste medio de los viajes, empresas de taxis con mayor volumen de negocio, zonas donde más viajes empiezan, zonas en las que terminan más viajes. 
* Consultar la información, tanto la tratada en tiempo real como la del histórico, desde una herramienta de visualización. Mostrando diferentes DashBoards.

## Arquitectura

* Para el almacenamiento y  procesamiento de los datos enviados por los taxis en tiempo real: Apache Kafka,  Apache Spark, Druid y S3.
* Para el alamcenamiento y procesamiento del histórico: Apache Spark, S3 y PostgreSQL.
* Para la visualización: Apache Superset.
 

![](https://github.com/albercn/tfm_taxitrips/blob/master/Arquitectura%20tfm_Taxitrips.jpg?raw=true)

## Formato de los mensajes
{
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
}

## Procesos Spark

* StreamingTaxiTrips.py: tiene como objetivo procesar los mensajes enviados, en formato JSON por los taxis en tiempo real, almacenandose en Druid, para su consulta desde Superset, y en S3 para su posterior procesamiento con el resto del histórico. 

* IngestHistoricTrips.py: tiene como objetivo almacenar en S3 los datos históricos de los viajes en taxi. 

* TransformTaxiTrips.py: tiene como objetivo procesar los viajes en taxi almacenados en S3, tanto por el batch como por el streaming, para almacenarlos en PostgreSQL y que puedan ser consultados desde Superset. 

* AreasLoc.py: tiene como objetivo generar el “maestro de areas”, que será usado para enriquecer tanto los datos en tiempo real como los históricos. 

## Tarea de Indexación Druid

* kafkaIngestionTaxiTrips.json: tarea de indexación que lee, de un topic de kafka, los datos enviados por los taxis, previamente procesados en el proceso Spark StreamingTaxiTrips.py.
