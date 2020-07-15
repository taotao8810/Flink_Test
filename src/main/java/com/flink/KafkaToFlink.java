package com.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.stream.Stream;

public class KafkaToFlink {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment Env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.58.177:9092");
        properties.setProperty("zookeeper.connect", "192.168.58.171:2181,192.168.58.177:2181");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("my_topic",new SimpleStringSchema(),properties);

        myConsumer.setStartFromLatest();
        myConsumer.setStartFromGroupOffsets();


        Env.setParallelism(2).setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String,Integer>> stream = Env.addSource(myConsumer)
                .flatMap((String lines, Collector<Tuple2<String,Integer>> out) ->
                        Stream.of(lines.split(","))
                        .forEach(a -> out.collect(Tuple2.of(a,1))))
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(0)
                //.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1)
                ;

        System.out.println("a");
        //stream.writeAsText("C:\\Users\\yaowentao\\Desktop\\a");
        stream.print();
        System.out.println("b");
        Env.execute("my first stream flink");
        System.out.println("c");

    }

}
