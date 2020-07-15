package com.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class TableStream {

    public static void main(String[] args) throws Exception {

        //创建flink运行环境
        StreamExecutionEnvironment Env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建tableEnvironment
        StreamTableEnvironment TableEnv =  StreamTableEnvironment.create(Env);

//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "192.168.58.177:9092");
//        properties.setProperty("zookeeper.connect", "192.168.58.171:2181,192.168.58.177:2181");
//        properties.setProperty("group.id", "test");

        //FlinkKafkaConsumer myConsumer = new FlinkKafkaConsumer("flink_topic",new SimpleStringSchema(),properties);

//         TableEnv.sqlUpdate("CREATE TABLE myfirst (\n" +
//                 "  a INT,\n" +
//                 "  b INT,\n" +
//                 "  c INT\n" +
//                 ") WITH (\n" +
//                 "  'connector.type' = 'filesystem',\n" +
//                 "  'connector.path' = 'C:\\Users\\yaowentao\\Desktop\\b.csv',\n" +
//                 "  'format.type' = 'csv'\n" +
//                 ")"
//         ) ;

        TableEnv.sqlUpdate("CREATE TABLE user_log1 (\n" +
                "    user_id VARCHAR,\n" +
                "    item_id VARCHAR,\n" +
                "    category_id VARCHAR,\n" +
                "    behavior VARCHAR,\n" +
                "    ts VARCHAR\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = 'universal',\n" +
                "    'connector.topic' = 'my-topic-one',\n" +
                "    'connector.startup-mode' = 'specific-offsets',\n" +
                "    'connector.specific-offsets' = 'partition:0,offset:27', \n" +
                "    'connector.properties.group.id' = 'testGroup',\n" +
                "    'connector.properties.zookeeper.connect' = '192.168.58.171:2181,192.168.58.177:2181,192.168.58.178:2181',\n" +
                "    'connector.properties.bootstrap.servers' = '192.168.58.177:9092',\n" +
                "    'format.type' = 'json'\n" +
                ")"
        ) ;

         Table result=TableEnv.sqlQuery("select user_id,count(*) from user_log1 group by user_id");
         //TableEnv.toAppendStream(result, Types.TUPLE(Types.INT,Types.LONG)).print();

         TableEnv.toRetractStream(result,Types.TUPLE(Types.STRING,Types.LONG)) .print();

         Env.execute("flink job1");
        //设置并行度
        //myConsumer.setStartFromEarliest();

        //添加数据源,json格式
        //DataStreamSource<ObjectNode> stream = Env.addSource(myConsumer);

        //stream.print();

    }
}
