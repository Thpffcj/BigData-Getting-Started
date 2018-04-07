package cn.edu.nju.intergration.jdbc;

import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by Thpffcj on 2018/4/7.
 * 使用Storm完成词频统计功能
 */
public class LocalWordCountJDBCStormTopology {

    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        public static final String[] words = new String[]{"apple", "orange", "pineapple", "banana", "watermelon"};

        public void nextTuple() {
            Random random = new Random();
            String word = words[random.nextInt(words.length)];

            this.collector.emit(new Values(word));
            System.out.println("emit: " + word);

            Utils.sleep(1000);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }

    /**
     * 对数据进行分割
     */
    public static class SplitBolt extends BaseRichBolt {

        private OutputCollector collector;

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        /**
         * 业务逻辑：
         */
        public void execute(Tuple input) {
            String word = input.getStringByField("line");
            this.collector.emit(new Values(word));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    /**
     * 词频汇总Bolt
     */
    public static class CountBolt extends  BaseRichBolt {

        private OutputCollector collector;

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        Map<String,Integer> map = new HashMap<String, Integer>();
        /**
         * 业务逻辑：
         * 1 获取每个单词
         * 2 对所有单词进行汇总
         * 3 输出
         */
        public void execute(Tuple input) {
            // 1）获取每个单词
            String word = input.getStringByField("word");
            Integer count = map.get(word);
            if(count == null) {
                count = 0;
            }

            count ++;

            // 2）对所有单词进行汇总
            map.put(word, count);

            this.collector.emit(new Values(word, map.get(word)));

        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "word_count"));
        }
    }

    public static void main(String[] args) {

        // 通过TopologyBuilder根据Spout和Bolt构建Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SplitBolt", new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt", new CountBolt()).shuffleGrouping("SplitBolt");

        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/storm");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","000000");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        String tableName = "wc";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withTableName(tableName)
                .withQueryTimeoutSecs(30);

        builder.setBolt("JdbcInsertBolt", userPersistanceBolt).shuffleGrouping("CountBolt");

        // 创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountJDBCStormTopology", new Config(), builder.createTopology());
    }
}