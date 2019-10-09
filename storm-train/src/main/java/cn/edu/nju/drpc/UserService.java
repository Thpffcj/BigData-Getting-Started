package cn.edu.nju.drpc;

/**
 * Created by Thpffcj on 2018/4/6.
 * 用户的服务
 */
public interface UserService {

    public static final long versionID = 88888888;

    /**
     * 添加用户
     * @param name 名字
     * @param age 年龄
     */
    public void addUser(String name, int age);
}
