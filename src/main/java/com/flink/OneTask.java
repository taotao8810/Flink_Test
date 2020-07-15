package com.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

public class OneTask {
    public static void main(String[] args) throws Exception{
//        Configuration conf = new Configuration(){
//            @Override
//            public void setInteger(String key, int value) {
//                super.setInteger("local-port", 9090);
//            }
//            @Override
//            public void setBoolean(String key, boolean value) {
//                super.setBoolean("web", true);
//            }
//        };

        //创建stream的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定时间窗口
        env.setParallelism(4).setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //读取文件，导入数据
        env//.socketTextStream("192.168.58.170",9001)
        .readTextFile("./data/word.txt")
                .flatMap((String line, Collector<Tuple2<String,Integer>> out) ->
                        Stream.of(line.split(","))
                                .forEach(a->out.collect(Tuple2.of(a,1)))
                ).returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1)
                .print();

        //env.execute("job name is WordCountStream1");
        env.execute("test");
    }

}
