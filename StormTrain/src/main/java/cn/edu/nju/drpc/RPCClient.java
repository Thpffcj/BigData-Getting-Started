package cn.edu.nju.drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by Thpffcj on 2018/4/6.
 * RPC 客户端
 */
public class RPCClient {

    public static void main(String[] args) throws IOException {

        Configuration configuration = new Configuration();

        long clientVersion = 88888888;

        UserService userService = RPC.getProxy(UserService.class, clientVersion,
                new InetSocketAddress("localhost", 9999),
                configuration);

        userService.addUser("Thpffcj", 21);
        System.out.println("From client invoked");

        RPC.stopProxy(userService);
    }
}
