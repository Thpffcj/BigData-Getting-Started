package cn.edu.nju.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Thpffcj on 2018/4/6.
 * 本地的DRPC
 */
public class LocalDRPCTopology {

    public static class MyBolt extends BaseRichBolt {

        private OutputCollector outputCollector;

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.outputCollector = collector;
        }

        public void execute(Tuple input) {

            // 请求的id
            Object requestId = input.getValue(0);
            // 请求的参数
            String name = input.getString(1);

            /**
             * TODO... 业务逻辑处理
             */
            String result = "add user: " + name;

            this.outputCollector.emit(new Values(requestId, result));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }
    }

    public static void main(String[] args) {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("addUser");
        builder.addBolt(new MyBolt());

        LocalCluster localCluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        localCluster.submitTopology("local-drpc", new Config(),
                builder.createLocalTopology(drpc));

        String result = drpc.execute("addUser", "Thpffcj");
        System.out.println("From client: " + result);

        localCluster.shutdown();
        drpc.shutdown();
    }
}
