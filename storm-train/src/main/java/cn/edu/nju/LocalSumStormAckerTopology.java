package cn.edu.nju;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Created by Thpffcj on 2018/4/4.
 * 使用Storm实现累计求和的操作
 */
public class LocalSumStormAckerTopology {

    /**
     * Spout需要继承BaseRichSpout
     * 数据源需要产生数据并发射
     */
    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;

        /**
         * 初始化方法，只会被调用一次
         *
         * @param conf      配置参数
         * @param context   上下文
         * @param collector 数据发射器
         */
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        int number = 0;

        /**
         * 会产生数据，在生产上肯定是从消息队列中获取数据
         * 这个方法是一个死循环，会一直不停的执行
         */
        public void nextTuple() {
            ++number;
            /**
             * emit方法有两个参数：
             *  1） 数据
             *  2） 数据的唯一编号 msgId
             *  如果是数据库，msgId就可以采用表中的主键
             */
            this.collector.emit(new Values(number), number);
            System.out.println("Spout: " + number);

            // 防止数据产生太快
            Utils.sleep(1000);
        }

        @Override
        public void ack(Object msgId) {
            System.out.println(" ack invoked ..." + msgId);
        }

        @Override
        public void fail(Object msgId) {
            System.out.println(" fail invoked ..." + msgId);

            // TODO... 此处对失败的数据进行重发或者保存下来
            // this.collector.emit(tuple)
            // this.dao.saveMsg(msgId)
        }

        /**
         * 声明输出字段
         *
         * @param declarer
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("num"));
        }
    }

    /**
     * 数据的累积求和Bolt：接收数据并处理
     */
    public static class SumBolt extends BaseRichBolt {

        private OutputCollector collector;

        /**
         * 初始化方法，会被执行一次
         *
         * @param stormConf
         * @param context
         * @param collector
         */
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        int sum = 0;

        /**
         * 其实也是一个死循环，职责：获取Spout发送过来的数据
         *
         * @param input
         */
        public void execute(Tuple input) {
            // Bolt中获取值可以根据index获取，也可以根据上一个环节中定义的field的名称获取(建议使用该方式)
            Integer value = input.getIntegerByField("num");
            sum += value;

            // 假设大于10的就是失败
            if (value > 0 && value <= 10) {
                this.collector.ack(input); // 确认消息处理成功
            } else {
                this.collector.fail(input);  // 确认消息处理失败
            }

//            try {
//                // TODO... 你的业务逻辑
//                this.collector.ack(input);
//            } catch (Exception e) {
//                this.collector.fail(input);
//            }

            System.out.println("Bolt sum: " + sum);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {

        // TopologyBuilder根据Spout和Bolt来构建出Topology
        // Storm中任何一个作业都是通过Topology的方式进行提交的
        // Topology中需要指定Spout和Bolt的执行顺序
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SumBolt", new SumBolt()).shuffleGrouping("DataSourceSpout");


        // 创建一个本地Storm集群：本地模式运行，不需要搭建Storm集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalSumStormAckerTopology", new Config(),
                builder.createTopology());
    }
}
