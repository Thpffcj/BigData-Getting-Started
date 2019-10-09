package cn.edu.nju.drpc;

import org.apache.storm.Config;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.DRPCClient;

/**
 * Created by Thpffcj on 2018/4/6.
 * Remote DRPC 客户端测试类
 */
public class RemoteDRPCClient {

    public static void main(String[] args) throws Exception {

        Config config = new Config();
        config.put("storm.thrift.transport", "org.apache.storm.security.auth.SimpleTransportPlugin");
        config.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);
        config.put(Config.DRPC_MAX_BUFFER_SIZE, 1048576);

        DRPCClient client = new DRPCClient(config, "thpffcj", 3772);
        String result = client.execute("addUser", "Thpffcj");

        System.out.println("Client invoked: " + result);
    }
}
