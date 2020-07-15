package com.flink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.Stream;


public class KafkaTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment Env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.58.177:9092");
        properties.setProperty("zookeeper.connect", "192.168.58.171:2181,192.168.58.177:2181");
        properties.setProperty("group.id", "test");

        //FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("flink_topic",new SimpleStringSchema(),properties);

        //读取Kafka中topic中的数据
        FlinkKafkaConsumer myConsumer = new FlinkKafkaConsumer("flink_topic",new JSONKeyValueDeserializationSchema(true),properties);

        //FlinkKafkaConsumer myConsumer = new FlinkKafkaConsumer("flink_topic",new SimpleStringSchema(),properties);


        //设置并行度
        myConsumer.setStartFromEarliest();

        //添加数据源,json格式
//        DataStreamSource<String> stream = Env.addSource(myConsumer);
//
//        DataStream<DataS> a = stream.map(new MapFunction<ObjectNode, DataS>() {
//            @Override
//            public DataS map(ObjectNode value) throws Exception {
//                DataS s1 = new DataS();
//                s1.setId(value.get("value").get("payload").get("data").get("ID").asInt());
//                s1.setName(value.get("value").get("payload").get("data").get("NAME").asText());
//                System.out.println(s1.getId() );
//                System.out.println(s1.getName());
//                return  s1;
//            }
//        }) ;

//        DataStream<DataS> b = stream.flatMap(new FlatMapFunction<ObjectNode, DataS> () {
//            @Override
//            public void flatMap(ObjectNode value, Collector<DataS> out) throws Exception {
//                DataS s1 = new DataS();
//                s1.setId(value.get("value").get("payload").get("data").get("ID").asInt());
//                s1.setName(value.get("value").get("payload").get("data").get("NAME").asText());
//                System.out.println(s1.getId() );
//                System.out.println(s1.getName());
//            }
//        });

        //b.print();

        //stream.print();
        // execute program
        Env.execute("flink_topic");



    }


    public static class DataS{

        public Integer id;
        public String name;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

    }
}
