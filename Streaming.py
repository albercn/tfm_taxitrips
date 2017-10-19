#!/usr/bin/env python
# Este archivo usa el encoding: utf-8

from __future__ import print_function

import sys
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from kafka import KafkaProducer


# Función para enviar el contenidod de cada partición al topic de salida de kafka
def sendPartition(particion):

    productor = KafkaProducer(bootstrap_servers=brokers)

    for r in particion:
        # INCLUIR TRASNFORMACIONES
        productor.send('outTopic', str(r[1]))

    productor.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: Streaming.py.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "StreamingTaxis")
ssc = StreamingContext(sc, 1)

# Creación del Input Direct DStream de kafka leyendo del receiver de kafka
brokers, topic = sys.argv[1:]
kst = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

kst.count().pprint()

kst.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))

# Inicio
ssc.start()
ssc.awaitTermination()
