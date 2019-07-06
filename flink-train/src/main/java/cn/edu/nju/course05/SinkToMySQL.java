package cn.edu.nju.course05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Created by thpffcj on 2019-07-05.
 */
public class SinkToMySQL extends RichSinkFunction<Student> {

    Connection connection;
    PreparedStatement preparedStatement;

    private Connection getConnection() {
        Connection conn = null;
        try {
            String url = "jdbc:mysql://localhost:3306/test";
            conn = DriverManager.getConnection(url, "root", "00000000");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        connection = getConnection();
        String sql = "insert into Student(id, name, age) values (?, ?, ?)";
        preparedStatement = connection.prepareStatement(sql);
    }

    // 每条记录插入时调用一次
    public void invoke(Student value, Context context) throws Exception {

        // 为前面的占位符赋值
        preparedStatement.setInt(1, value.getId());
        preparedStatement.setString(2, value.getName());
        preparedStatement.setInt(3, value.getAge());

        preparedStatement.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if(connection != null) {
            try {
                connection.close();
            } catch(Exception e) {
                e.printStackTrace();
            }
            connection = null;
        }
    }
}
