package cn.edu.nju.project

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.elasticsearch.client.Requests
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.slf4j.LoggerFactory

/**
  * Created by thpffcj on 2019-07-10.
  */
object LogAnalysis {

  // 在生产上记录日志建议采用这种方式
  val logger = LoggerFactory.getLogger("LogAnalysis")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val topic = "test"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test-group")

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)

    // 接收kafka数据
    val data = env.addSource(consumer)

    val logData = data.map(x => {
      val splits = x.split("\t")
      val level = splits(2)
      val timeStr = splits(3)
      var time = 0l

      try {
        val sourceFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        time = sourceFormat.parse(timeStr).getTime
      } catch {
        case e: Exception => {
          logger.error(s"time parse error: $timeStr", e.getMessage)
        }
      }

      val domain = splits(5)
      val traffic = splits(6).toLong

      (level, time, domain, traffic)
    }).filter(_._2 != 0).filter(_._1 == "E")
        .map(x => {
          (x._2, x._3, x._4)  // 1 level(抛弃) 2 time 3 domain 4 traffic
        })

//    logData.print().setParallelism(1)

    /**
      * 在生产上进行业务处理的时候，一定要考虑处理的健壮性以及你数据的准确性
      * 脏数据或者是不符合业务规则的数据是需要全部过滤掉之后
      * 再进行业务逻辑的处理
      *
      * 对于我们的业务来说，我们只需要统计level=E的即可
      * 对于level非E的，不作为我们业务指标的统计范畴
      * 数据清洗：就是按照我们的业务规则把原始输入的数据进行一定业务规则的处理
      * 使得满足我们的业务需求为准
      */

    val resultData = logData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Long)] {

      // 允许数据的最大乱序时间
      val maxOutOfOrderness =   10000L

      var currentMaxTimestamp: Long = _

      // 获取当前水位线
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      // 从数据本身中提取EventTime
      override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    }).keyBy(1)  // 此处是按照域名进行KeyBy的
        .window(TumblingEventTimeWindows.of(Time.seconds(60)))
        .apply(new WindowFunction[(Long, String, Long), (String, String, Long), Tuple, TimeWindow] {
          override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {

            val domain = key.getField(0).toString
            var sum = 0l

            val iterator = input.iterator
            var inputTime = 0l
            while (iterator.hasNext) {
              val next = iterator.next()
              sum += next._3  // traffic求和
              inputTime = next._1
            }

            val resultFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
            val time = resultFormat.format(inputTime)

            /**
              * 第一个参数：这一分钟的时间 2019-09-09 20:20
              * 第二个参数：域名
              * 第三个参数：traffic的和
              */
            out.collect((time, domain, sum))
          }
        })

    resultData.print().setParallelism(1)

    // ES部分
    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[(String, String, Long)](
      httpHosts,
      new ElasticsearchSinkFunction[(String, String, Long)] {
        def createIndexRequest(element: (String, String, Long)): IndexRequest = {
          val json = new java.util.HashMap[String, Any]
          json.put("time", element._1)
          json.put("domain", element._2)
          json.put("traffics", element._3)

          val id = element._1 + "-" + element._2

          return Requests.indexRequest()
            .index("cdn")
            .`type`("traffic")
            .id(id)
            .source(json)
        }

        override def process(t: (String, String, Long), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(t))
        }
      }
    )

    // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
    esSinkBuilder.setBulkFlushMaxActions(1)

    // finally, build and add the sink to the job's pipeline
    resultData.addSink(esSinkBuilder.build)

    env.execute("LogAnalysis")
  }
}
