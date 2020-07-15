package com.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import java.util.stream.Stream;

public class TestWordCount {
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration (){{
            setInteger("rest.port",9191);
            setBoolean("local.start-webserver",true);
        }};

        ExecutionEnvironment batchEnv=ExecutionEnvironment .createLocalEnvironmentWithWebUI(conf);
        batchEnv.fromElements(
                "apache flink",
                "apache stream",
                "apache sql",
                "apache batch",
                "apache graph"
        ).flatMap((String line,Collector<Tuple2<String,Integer>> out) ->
                Stream.of(line.split("\\t"))
                        .forEach(value -> out.collect(Tuple2.of(value,1)))
            ).returns(Types.TUPLE(Types.STRING,Types.INT))
    .groupBy(0)
                .sum(1)
                .print()
                ;

    }

}
