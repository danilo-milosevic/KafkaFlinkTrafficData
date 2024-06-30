from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, window, avg
from pyspark.sql.streaming import StreamingQueryException
from pyspark.sql.types import DoubleType, IntegerType
import sys
from pyspark.sql.functions import col, avg, udf, window, lit, concat, unix_timestamp, from_unixtime


import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

class EmissionConsumerSpark:

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
        EmissionConsumerSpark.xZones = xZones
        EmissionConsumerSpark.yZones = yZones
        EmissionConsumerSpark.blockSizeX = (EmissionConsumerSpark.maxX - EmissionConsumerSpark.minX) / EmissionConsumerSpark.xZones
        EmissionConsumerSpark.blockSizeY = (EmissionConsumerSpark.maxY - EmissionConsumerSpark.minY) / EmissionConsumerSpark.yZones

    @staticmethod
    def determine_zone(x, y):
        xInd = int((x - EmissionConsumerSpark.minX) / EmissionConsumerSpark.blockSizeX)
        yInd = int((y - EmissionConsumerSpark.minY) / EmissionConsumerSpark.blockSizeY)
        return yInd * EmissionConsumerSpark.xZones + xInd

    def get_window(self, windowSeconds, slideWindowSeconds = None):
        if slideWindowSeconds is None:
            return window(col("timestamp"), f"{windowSeconds} seconds").alias("window")
        return window(col("timestamp"), f"{windowSeconds} seconds", f"{slideWindowSeconds} seconds").alias("window")

    @staticmethod
    def format_console_output(row):
        split = row[0].split(';')
        formatted_output = "Zone: {}, Window Start: {}, Window End: {}, Avg CO: {}, Avg CO2: {}, Avg HC: {}, Avg NOx: {}, Avg PMx: {}".format(
            split[0], split[1], split[2],split[3],split[4],split[5],split[6],split[7]
        )
        return formatted_output
        
    def consume(self, config):
        spark = SparkSession.builder.appName("EMS").master("local[*]").getOrCreate()

        kafkaDF = spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", "kafka:9092")\
            .option("subscribe", config['topic'])\
            .option("startingOffsets", "earliest")\
            .load()

        emissionsStream = kafkaDF.selectExpr("CAST(value AS STRING)").alias("value")

        parsedData = emissionsStream.selectExpr("split(value, ';') as parsed")\
            .selectExpr(
                "parsed[0] AS timestamp",
                "parsed[1] AS CO",
                "parsed[2] AS CO2",
                "parsed[3] AS HC",
                "parsed[4] AS NOx",
                "parsed[5] AS PMx",
                "parsed[18] AS x",
                "parsed[19] AS y"
            )

        emissionsData = parsedData.select(
            col("timestamp").cast("timestamp"),
            col("CO").cast(DoubleType()),
            col("CO2").cast(DoubleType()),
            col("HC").cast(DoubleType()),
            col("NOx").cast(DoubleType()),
            col("PMx").cast(DoubleType()),
            col("x").cast(DoubleType()),
            col("y").cast(DoubleType())
        )
        
        determine_zone_udf = udf(self.determine_zone, IntegerType())
        emissionsData = emissionsData.withColumn("zone", determine_zone_udf(col("x"), col("y")))

        reducedData = emissionsData.groupBy(
            col("zone"),
            self.get_window(config['secondsWindow'], slideWindowSeconds=config['slideWindowSeconds'])
        ).agg(
            avg("CO").alias("avg_CO"),
            avg("CO2").alias("avg_CO2"),
            avg("HC").alias("avg_HC"),
            avg("NOx").alias("avg_NOx"),
            avg("PMx").alias("avg_PMx")
        )

        windowData = reducedData.withColumn("window_start", from_unixtime(unix_timestamp(col("window.start"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")) \
                                    .withColumn("window_end", from_unixtime(unix_timestamp(col("window.end"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss"))\
                                    .drop('window')
            
        formated_data = windowData.withColumn("value", concat(
                col("zone"), lit(";"),
                col("window_start"), lit(";"),
                col("window_end"), lit(";"),
                col("avg_CO"), lit(";"),
                col("avg_CO2"), lit(";"),
                col("avg_HC"), lit(";"),
                col("avg_NOx"), lit(";"),
                col("avg_PMx")
            )).selectExpr("CAST(value AS STRING)")
        
        producer = KafkaProducer(bootstrap_servers='kafka:9092')
        query = formated_data \
                .writeStream \
                .outputMode("complete") \
                .foreachBatch(lambda batch_df, batch_id: 
                    [(producer.send(config['out_topic'], value=row.value.encode('utf-8'))) for row in batch_df.collect() if config['out_topic'] is not None]+
                    [print(EmissionConsumerSpark.format_console_output(row)) for row in batch_df.collect()]
                ) \
                .start()
        query.awaitTermination()
        producer.close()

    @staticmethod
    def get_app_config(args):
        config = {
            'topic':'emsTopic',
            'group':'emission-group',
            'to_print':False,
            'out_topic':None,
            'secondsWindow':5,
            'slideWindowSeconds':1,#s
            'xZones':4,
            'yZones': 4,
            'error':False
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
                config['out_topic'] = "ems_out_topic_spark"
                i+=1
            
            config['xZones'] = int(args[i])
            i+=1

            config['yZones'] = int(args[i])
            i+=1
        except:
            pass
        return config

if __name__ == "__main__":
    c = EmissionConsumerSpark.get_app_config(sys.argv)

    emsConsumer = EmissionConsumerSpark(c['topic'],c['xZones'],c['yZones'])

    try:
        emsConsumer.consume(c)
    except Exception as e:
        print(e)
