package cn.edu.nju.course08

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode

/**
  * Created by thpffcj on 2019-07-07.
  */
object KafkaConnectorConsumerApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint常用设置参数
    env.enableCheckpointing(4000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    val topic = "test"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")
    val data = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties))

    data.print()

    env.execute("KafkaConnectorConsumerApp")
  }
}
