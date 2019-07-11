package cn.edu.nju.project;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Created by thpffcj on 2019-07-10.
 */
public class MyKafkaProducer {

    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        String topic = "test";

        // 通过死循环一直不停的往Kafka的Broker里面生产数据
        while (true) {

            StringBuilder builder = new StringBuilder();

            builder.append("aliyun").append("\t")
                    .append("CN").append("\t")
                    .append(getLevels()).append("\t")
                    .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("\t")
                    .append(getIps()).append("\t")
                    .append(getDomains()).append("\t")
                    .append(getTraffic()).append("\t");

            System.out.println(builder.toString());
            producer.send(new ProducerRecord<String, String>(topic, builder.toString()));

            Thread.sleep(2000);
        }
    }

    private static long getTraffic() {
        return new Random().nextInt(10000);
    }

    private static String getDomains() {
        String[] domains = new String[] {
                "v1.go2yd.com",
                "v2.go2yd.com",
                "v3.go2yd.com",
                "v4.go2yd.com",
                "vmi.go2yd.com"
        };
        return domains[new Random().nextInt(domains.length)];
    }

    private static String getIps() {
        String[] ips = new String[] {
                "223.104.18.110",
                "113.101.75.194",
                "27.17.127.135",
                "183.225.139.16",
                "112.1.66.34",
                "175.148.211.190",
                "183.227.58.21",
                "59.83.198.84",
                "117.28.38.28",
                "117.59.39.169"
        };
        return ips[new Random().nextInt(ips.length)];
    }

    // 生产level数据
    public static String getLevels() {
        String[] levels = new String[]{"M", "E"};
        return levels[new Random().nextInt(levels.length)];
    }
}
