package cn.edu.nju;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用Java API来开发Flink的实时处理应用程序
 * wc统计的数据我们源自于socket
 * Created by thpffcj on 2019-06-28.
 */
public class StreamingWCJavaApp {

    public static void main(String[] args) throws Exception {

        // 获取参数
        int port = 0;

        try {
            ParameterTool tool = ParameterTool.fromArgs(args);
            port = tool.getInt("port");
        } catch (Exception e) {
            System.err.println("端口未设置，使用默认端口9999");
            port = 9999;
        }

        // step1：获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // step2：读取数据
        DataStreamSource<String> text = env.socketTextStream("localhost", port);

        // step3：transform
//        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                String[] tokens = value.toLowerCase().split(",");
//                for (String token : tokens) {
//                    if (token.length() > 0) {
//                        collector.collect(new Tuple2<String, Integer>(token, 1));
//                    }
//                }
//            }
//        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print();

//        text.flatMap(new FlatMapFunction<String, WC>() {
//            @Override
//            public void flatMap(String value, Collector<WC> collector) throws Exception {
//                String[] tokens = value.toLowerCase().split(",");
//                for (String token : tokens) {
//                    if (token.length() > 0) {
//                        collector.collect(new WC(token, 1));
//                    }
//                }
//            }
//        }).keyBy("word").timeWindow(Time.seconds(5)).sum("count").print();

        text.flatMap(new FlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String value, Collector<WC> collector) throws Exception {
                String[] tokens = value.toLowerCase().split(",");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(new WC(token, 1));
                    }
                }
            }
        }).keyBy(new KeySelector<WC, String>() {
            @Override
            public String getKey(WC wc) throws Exception {
                return wc.word;
            }
        }).timeWindow(Time.seconds(5)).sum("count").print();

        env.execute("StreamingWCJavaApp");
    }

    public static class WC {

        private String word;
        private int count;

        public WC() {}

        public WC(String word, int count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
