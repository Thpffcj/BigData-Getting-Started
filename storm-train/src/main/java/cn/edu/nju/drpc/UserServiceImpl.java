package cn.edu.nju.drpc;

/**
 * Created by Thpffcj on 2018/4/6.
 * 用户的服务接口实现类
 */
public class UserServiceImpl implements UserService {

    public void addUser(String name, int age) {
        System.out.println("From Server Invoked: add user success, name is " + name);
    }
}
