package cn.edu.nju.spark;

import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;


/**
 * Created by Thpffcj on 2018/1/16.
 * 使用Java开发Spark Streaming应用程序
 */
public class StreamingWordCountApp {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("StreamingWordCountApp");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 创建一个DStream(hostname + port)
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
