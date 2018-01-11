package cn.edu.nju.spark.kafka;

/**
 * Created by Thpffcj on 2018/1/11.
 */
public class KafkaClientApp {

    public static void main(String[] args) {
        new KafkaProducer(KafkaProperties.TOPIC).start();
        new KafkaConsumer(KafkaProperties.TOPIC).start();
    }
}
