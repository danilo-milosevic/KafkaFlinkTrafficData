from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, window, desc
from pyspark.sql.types import DoubleType, IntegerType
import sys
from pyspark.sql.functions import col, udf, window, lit, concat, unix_timestamp, from_unixtime
import time

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

class FCDConsumerSpark:

    minY = -1.68
    minX = -74.273621
    maxX = 25447.74
    maxY = 36412.67

    xZones = 4  
    yZones = 4
    blockSizeX = None
    blockSizeY = None
    producer = None

    def __init__(self, topic, xZones, yZones):
        self.topic = topic
        FCDConsumerSpark.xZones = xZones
        FCDConsumerSpark.yZones = yZones
        FCDConsumerSpark.blockSizeX = (FCDConsumerSpark.maxX - FCDConsumerSpark.minX) / FCDConsumerSpark.xZones
        FCDConsumerSpark.blockSizeY = (FCDConsumerSpark.maxY - FCDConsumerSpark.minY) / FCDConsumerSpark.yZones

    @staticmethod
    def determine_zone(x, y):
        xInd = int((x - FCDConsumerSpark.minX) / FCDConsumerSpark.blockSizeX)
        yInd = int((y - FCDConsumerSpark.minY) / FCDConsumerSpark.blockSizeY)
        return yInd * FCDConsumerSpark.xZones + xInd

    def get_window(self, windowSeconds, slideWindowSeconds = None):
        if slideWindowSeconds is None:
            return window(col("timestamp"), f"{windowSeconds} seconds").alias("window")
        return window(col("timestamp"), f"{windowSeconds} seconds", f"{slideWindowSeconds} seconds").alias("window")

    def format_console_output(row):
        split = row[0].split(';')
        formatted_output = "Zone: {}, Window Start: {}, Window End: {}, Lane: {}, Count: {}".format(
            split[0], split[1], split[2],split[3],split[4]
        )
        return formatted_output
        
    def consume(self, config):
        start = time.time()
        spark = SparkSession.builder.appName("FCD").master(f"local[{config['num_proc']}]").getOrCreate()

        kafkaDF = spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", "kafka:9092")\
            .option("subscribe", config['topic'])\
            .option("startingOffsets", "earliest")\
            .load()

        emissionsStream = kafkaDF.selectExpr("CAST(value AS STRING)").alias("value")

        parsedData = emissionsStream.selectExpr("split(value, ';') as parsed")\
            .selectExpr(
                "parsed[0] AS timestamp",
                "substring_index(parsed[3], '#', 1) AS lane", 
                "parsed[8] AS x",
                "parsed[9] AS y"
            )

        emissionsData = parsedData.select(
            col("timestamp").cast("timestamp"),
            col("lane"),
            col("x").cast(DoubleType()),
            col("y").cast(DoubleType())
        )
        
        determine_zone_udf = udf(self.determine_zone, IntegerType())
        emissionsData = emissionsData.withColumn("zone", determine_zone_udf(col("x"), col("y")))

        reducedData = emissionsData.groupBy(
            col("lane"),
            col("zone"),
            self.get_window(config['secondsWindow'], slideWindowSeconds=config['slideWindowSeconds'])
        ).count().orderBy(desc("count"))

        windowData = reducedData.withColumn("window_start", from_unixtime(unix_timestamp(col("window.start"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")) \
                                    .withColumn("window_end", from_unixtime(unix_timestamp(col("window.end"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss"))\
                                    .drop('window')
            
        formated_data = windowData.withColumn("value", concat(
                col("zone"), lit(";"),
                col("window_start"), lit(";"),
                col("window_end"), lit(";"),
                col("lane"), lit(";"),
                col("count"), lit(";"),
            )).selectExpr("CAST(value AS STRING)")
        
        producer = KafkaProducer(bootstrap_servers='kafka:9092')
        try:
            query = formated_data \
                    .writeStream \
                    .outputMode("complete") \
                    .foreachBatch(lambda batch_df, batch_id: 
                        [(producer.send(config['out_topic'], value=row.value.encode('utf-8'))) for row in batch_df.collect() if config['out_topic'] is not None]+
                        [print(FCDConsumerSpark.format_console_output(row)) for row in batch_df.collect()]
                    )\
                    .start()
            query.awaitTermination()
        except:
            end = time.time()
            print("Time in s: ",(end-start))
        producer.close()

    @staticmethod
    def get_app_config(args):
        config = {
            'topic':'fcdTopic',
            'group':'fcd-group',
            'to_print':False,
            'out_topic':None,
            'secondsWindow':5,
            'slideWindowSeconds':1,#s
            'xZones':4,
            'yZones': 4,
            'error':False,
            'num_proc':'*'
        }
        i=1
        try:
            if (args[i]=="--print"):
                config['to_print'] = True
                i+=1

            config['secondsWindow'] = int(args[i])
            i+=1

            if(args[i]=="--slide"):
                i+=1
                config['slideWindowSeconds'] = int(args[i])
                i+=1
            
            if(args[i]=="--out"):
                config['out_topic'] = "fcd_out_topic_spark"
                i+=1

            if(args[i]=='--n'):
                i+=1
                try:
                    config['num_proc'] = str(int(args[i]))
                except:
                    config['num_proc'] = '*'
                i+=1
            
            config['xZones'] = int(args[i])
            i+=1

            config['yZones'] = int(args[i])
            i+=1
        except:
            pass
        return config

if __name__ == "__main__":
    c = FCDConsumerSpark.get_app_config(sys.argv)

    fcdConsumer = FCDConsumerSpark(c['topic'],c['xZones'],c['yZones'])

    try:
        fcdConsumer.consume(c)
    except Exception as e:
        print(e)
