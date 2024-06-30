import datetime
from typing import Any
from py4j.java_gateway import JavaObject
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows, TumblingProcessingTimeWindows, SlidingProcessingTimeWindows
from pyflink.common import Types, Time, Duration
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream import WindowFunction
from pyflink.datastream import DataStream
import datetime
import os
from pathlib import Path


class ProcessWindowFunction(WindowFunction):
    def __init__(self, func):
        self.func = func

    def apply(self, key, window, input_iterator):
        result = self.func(window, input_iterator)
        return [result]

class Assigner(TimestampAssigner):
    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return int(value[0])

class ConsumerFlink:
    minY = -1.68
    minX = -74.273621
    maxX = 25447.74
    maxY = 36412.67

    def __init__(self, topic, xZones, yZones):
        self.topic = topic
        self.xZones = xZones
        self.yZones = yZones
        self.blockSizeX = (self.maxX - self.minX) / xZones
        self.blockSizeY = (self.maxY - self.minY) / yZones

    def determine_zone(self, x, y):
        xInd = int((x - self.minX) / self.blockSizeX)
        yInd = int((y - self.minY) / self.blockSizeY)
        return yInd * self.xZones + xInd

    def get_window(self, windowSeconds, slideWindowSeconds = None):
        if slideWindowSeconds is None:
            return TumblingProcessingTimeWindows.of(Time.milliseconds(windowSeconds*1000))
        return SlidingProcessingTimeWindows.of(Time.milliseconds(windowSeconds*1000), Time.milliseconds(slideWindowSeconds*1000))

    def add_jars(self, env):
        file = "file://"
        jars = ["/flink/flink-python-1.19.0.jar",
                "/flink/flink-sql-connector-kafka-3.2.0-1.19.jar",
                "/flink/kafka-clients-3.2.0.jar",
                "/flink/flink-connector-kafka-3.2.0-1.19.jar"]

        for p in jars:
            if not os.path.isfile(Path(p)):
                print(p," doesn't exist!")
                exit(1)
            env.add_jars(file+p)

    def init_stream(self, config, env:StreamExecutionEnvironment)->DataStream:
        try:
            source = KafkaSource.builder() \
                .set_bootstrap_servers("kafka:9092") \
                .set_topics(config['topic']) \
                .set_group_id(config['group']) \
                .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
                .set_value_only_deserializer(SimpleStringSchema()) \
                .build()
            kafka_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
            return kafka_stream
        except Exception as e:
            print("Failed creating the consumer: ", e)
            exit(1)

    def process_ems(window, iterable):
        elements = list(iterable)
        zone = elements[0][8]
        window_start = window.start
        window_end = window.end
        avg_CO = round(sum([x[1] for x in elements]) / len(elements),2)
        avg_CO2 = round(sum([x[2] for x in elements]) / len(elements),2)
        avg_HC = round(sum([x[3] for x in elements]) / len(elements),2)
        avg_NOx = round(sum([x[4] for x in elements]) / len(elements),2)
        avg_PMx = round(sum([x[5] for x in elements]) / len(elements),2)
        res = str(f"|Zone: {zone};\tWindowStart: {window_start};\tWindowEnd: {window_end};\tCO: {avg_CO};\tCO2: {avg_CO2};\tHC: {avg_HC};\tNOx: {avg_NOx};\tPMx: {avg_PMx}|")
        return res
    
    def process_fcd(window, iterable):
        elements = list(iterable)
        zone = elements[0][4]
        window_start = window.start
        window_end = window.end
        lane = elements[0][1]
        count = len(elements)
        res = str(f"|Zone: {zone};\tWindowStart:{window_start};\tWindowEnd: {window_end};\tLane: {lane};\tCount: {count}|")
        return res
    
    def consume(self, config):
        env = StreamExecutionEnvironment.get_execution_environment()
        self.add_jars(env)

        kafka_stream = self.init_stream(config, env)

        xy_inds = config['xy_inds']
        type_ind_tuples = config['type_ind_tuples']

        def parse_data(line):
            fields = line.split(';')
            result = ()
            for t in type_ind_tuples:
                ind = t[0]
                ty = t[1]
                if ind >= len(fields):
                    print("Parsing went wrong, trying to access element ",ind," length is ",len(fields))
                    exit(1)
                value = fields[ind]
                if (len(t)>2):
                    value = value.split('#')[0]
                result += (ty(value),)
            return result

        def assign_zone(value):
            x, y = xy_inds
            zone = self.determine_zone(value[x], value[y])
            return (*value, zone)

        process_f = ConsumerFlink.process_ems if config['topic']=='emsTopic' else ConsumerFlink.process_fcd
        
        try:
            parsed_stream = kafka_stream.map(parse_data, output_type=config['stream_types'])
        except Exception as e:
            print("Failed creating the parsed stream: ",e)
            exit(1)     

        try:
            enriched_stream = parsed_stream.map(assign_zone, output_type=config['zoned_types'])
        except Exception as e:
            print("Failed adding the zone attribute: " , e)
            exit(1)
        
        try:
            windowed_stream = enriched_stream.assign_timestamps_and_watermarks(
                WatermarkStrategy.
                for_bounded_out_of_orderness(Duration.of_seconds(10)).
                with_timestamp_assigner(Assigner())
            ).key_by(config['key_by']).window(
                self.get_window(config['secondsWindow'], config.get('slideWindowSeconds'))
            ).apply(ProcessWindowFunction(process_f), output_type=Types.STRING())

            windowed_stream.print()
        except Exception as e:
            print("Failed adding windows: ", e)
            exit(1)

        if config['out_topic']:
            producer = FlinkKafkaProducer(
                topic=config['out_topic'],
                serialization_schema=SimpleStringSchema(),
                producer_config={'bootstrap.servers': 'kafka:9092'}
            )
            windowed_stream.add_sink(producer)
        
        try:
            env.execute()
        except Exception as e:
            print("Execute failed: ",e)

    @staticmethod
    def get_app_config(args):
        ems_type_ind_tuples = ((0,str),(1,float),(2,float),(3,float),(4,float),(5,float),(18,float),(19, float))
        fcd_type_ind_tuples = ((0,str),(1,str,'split'),(8,float),(9,float))
        ems_type = Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()])
        fcd_type = Types.TUPLE([Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE()])
        ems_zone = Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),Types.INT()])
        fcd_zone = Types.TUPLE([Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.INT()])
        config = {
            'topic': 'emsTopic',
            'group': 'emission-group',
            'to_print': False,
            'out_topic': None,
            'secondsWindow': 5,
            'slideWindowSeconds': None,
            'xZones': 4,
            'yZones': 4,
            'error': False,
            'type_ind_tuples': ems_type_ind_tuples,
            'xy_inds':(6,7),
            'stream_types':ems_type,
            'zoned_types':ems_zone,
            'key_by': lambda x: x[8]
        }

        i = 1
        try:
            if args[i] == "fcd":
                config['topic'] = 'fcdTopic'
                config['group'] = 'fcd-group'
                config['type_ind_tuples'] = fcd_type_ind_tuples
                config['xy_inds']=(2,3)
                config['stream_types'] = fcd_type
                config['zoned_types'] = fcd_zone
                config['key_by'] = lambda x: (x[1],x[4])
                i+=1
            
            if args[i] == "--print":
                config['to_print'] = True
                i += 1

            config['secondsWindow'] = int(args[i])
            i += 1

            if args[i] == "--slide":
                i += 1
                config['slideWindowSeconds'] = int(args[i])
                i += 1
            
            if args[i] == "--out":
                config['out_topic'] = 'ems_out_topic_flink'
                i += 1
            
            config['xZones'] = int(args[i])
            i += 1

            config['yZones'] = int(args[i])
            i += 1
        except:
            pass
        return config

if __name__ == "__main__":
    import sys
    config = ConsumerFlink.get_app_config(sys.argv)
    emsConsumer = ConsumerFlink(config['topic'], config['xZones'], config['yZones'])

    try:
        emsConsumer.consume(config)
    except Exception as e:
        print(e)