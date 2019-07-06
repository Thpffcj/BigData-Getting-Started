package cn.edu.nju.course05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by thpffcj on 2019-07-05.
 */
public class JavaDataStreamTransformationApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        filterFunction(env);
//        unionFunction(env);
        splitSelectFunction(env);

        env.execute("JavaDataStreamTransformationApp");
    }

    public static void splitSelectFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new JavaCustomNonParallelSourceFunction());

        SplitStream<Long> splits = data.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                List<String> output = new ArrayList<>();
                if (value % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        });

        splits.select("odd").print().setParallelism(1);
    }

    public static void unionFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data1 = env.addSource(new JavaCustomNonParallelSourceFunction());
        DataStreamSource<Long> data2 = env.addSource(new JavaCustomNonParallelSourceFunction());
        data1.union(data2).print().setParallelism(1);
    }

    public static void filterFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new JavaCustomNonParallelSourceFunction());
        data.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("receive: " + value);
                return value;
            }
        }).filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        }).print().setParallelism(1);
    }
}
