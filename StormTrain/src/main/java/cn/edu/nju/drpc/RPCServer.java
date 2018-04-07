package cn.edu.nju.drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * Created by Thpffcj on 2018/4/6.
 * RPC Server服务
 */
public class RPCServer {

    public static void main(String[] args) throws IOException {

        Configuration configuration = new Configuration();

        RPC.Builder builder = new RPC.Builder(configuration);

        // Java Builder 模式
        RPC.Server server = builder.setProtocol(UserService.class)
                .setInstance(new UserServiceImpl())
                .setBindAddress("localhost").setPort(9999).build();


        server.start();
    }
}
