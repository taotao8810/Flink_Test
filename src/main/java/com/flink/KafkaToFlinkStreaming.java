package com.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class KafkaToFlinkStreaming {

    public static void main(String[] args) throws Exception {

        //创建flink运行环境
        StreamExecutionEnvironment Env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建tableEnvironment
        StreamTableEnvironment TableEnv =  StreamTableEnvironment.create(Env);

        TableEnv.sqlUpdate("CREATE TABLE user_log1 (\n" +
                "    user_id VARCHAR,\n" +
                "    item_id VARCHAR,\n" +
                "    category_id VARCHAR,\n" +
                "    behavior VARCHAR,\n" +
                "    ts TIMESTAMP\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = 'universal',\n" +
                "    'connector.topic' = 'test-topic3',\n" +
                "    'connector.startup-mode' = 'earliest-offset',\n" +
                "    'connector.properties.group.id' = 'testGroup',\n" +
                "    'connector.properties.zookeeper.connect' = '192.168.58.171:2181,192.168.58.177:2181,192.168.58.178:2181',\n" +
                "    'connector.properties.bootstrap.servers' = '192.168.58.177:9092',\n" +
                "    'format.type' = 'json'\n" +
                ")"
        ) ;

        Table result=TableEnv.sqlQuery("select user_id,count(1) from user_log1 group by user_id");

        TableEnv.toRetractStream(result, Types.TUPLE(Types.STRING,Types.LONG)) .print();
        Env.execute("flink streaming job");
    }
}
