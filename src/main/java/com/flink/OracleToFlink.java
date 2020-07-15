package com.flink;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class OracleToFlink {
    public static void main(String[] args) throws Exception {

        //创建flink运行环境
        StreamExecutionEnvironment Env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建tableEnvironment
        StreamTableEnvironment TableEnv =  StreamTableEnvironment.create(Env);

        TableEnv.sqlUpdate("CREATE TABLE user_log2 (\n" +
                "    payload ROW(SCN string,SEG_OWNER string,data ROW(ID DECIMAL,NAME string))\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = 'universal',\n" +
                "    'connector.topic' = 'flink_topic',\n" +
                "    'connector.startup-mode' = 'earliest-offset',\n" +
                "    'connector.properties.group.id' = 'test',\n" +
                "    'connector.properties.zookeeper.connect' = '192.168.58.171:2181,192.168.58.177:2181,192.168.58.178:2181',\n" +
                "    'connector.properties.bootstrap.servers' = '192.168.58.177:9092',\n" +
                "    'format.type' = 'json',\n" +
                "    'format.json-schema' =\n" +
                "    '{\n" +
                "        type : \"object\",\n" +
                "        \"properties\":\n" +
                "        {\n" +
                "            \"payload\": {type : \"object\",\n" +
                "                \"properties\": \n" +
                "                 {\n" +
                "                    \"SCN\" : {type :\"string\"},\n" +
                "                    \"SEG_OWNER\" : {type :\"string\"},\n" +
                "                    \"data\": {type : \"object\",\n" +
                "                    \"properties\": {\"ID\": {type : \"integer\"},\n" +
                "                                   \"NAME\": {type : \"string\"}\n" +
                "                                  }\n" +
                "                             }\n" +
                "                 }}\n" +
                "        }\n" +
                "    }'\n" +
                ")"
        ) ;

        Table result=TableEnv.sqlQuery("select payload.data.NAME,sum(payload.data.ID) from user_log2 group by payload.data.NAME");

        TableEnv.toRetractStream(result,Types.TUPLE(Types.STRING,Types.BIG_DEC)) .print();
        Env.execute("flink job");
    }
}
