package cn.edu.nju.course05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by thpffcj on 2019-07-05.
 */
public class JavaCustomSinkToMySQL {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<Student> studentStream = source.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                System.out.println(value);
                String[] splits = value.split(",");
                Student stu = new Student();
                stu.setId(Integer.parseInt(splits[0]));
                stu.setName(splits[1]);
                stu.setAge(Integer.parseInt(splits[2]));
                return stu;
            }
        });

        studentStream.addSink(new SinkToMySQL());

        env.execute("JavaCustomSinkToMySQL");
    }
}
