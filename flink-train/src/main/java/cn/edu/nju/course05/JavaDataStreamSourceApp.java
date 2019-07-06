package cn.edu.nju.course05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by thpffcj on 2019-07-04.
 */
public class JavaDataStreamSourceApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        socketFunction(env);
//        nonParallelSourceFunction(env);
//        parallelSourceFunction(env);
        richParallelSourceFunction(env);

        env.execute("JavaDataStreamSourceApp");
    }

    public static void richParallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new JavaCustomRichParallelSourceFunction()).setParallelism(2);
        data.print().setParallelism(1);
    }

    public static void parallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new JavaCustomParallelSourceFunction()).setParallelism(2);
        data.print().setParallelism(1);
    }

    public static void nonParallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new JavaCustomNonParallelSourceFunction());
        data.print().setParallelism(1);
    }

    public static void socketFunction(StreamExecutionEnvironment env) {
        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);
        data.print().setParallelism(1);
    }
}
