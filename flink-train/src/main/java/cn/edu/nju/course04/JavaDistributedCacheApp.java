package cn.edu.nju.course04;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by thpffcj on 2019-07-04.
 */
public class JavaDistributedCacheApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "file:///Users/thpffcj/Public/data/hello.txt";

        // step1：注册一个本地/HDFS文件
        env.registerCachedFile(filePath, "java-dc");

        DataSource<String> data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm");

        data.map(new RichMapFunction<String, String>() {

            List<String> list = new ArrayList<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                File file = getRuntimeContext().getDistributedCache().getFile("java-dc");
                List<String> lines = FileUtils.readLines(file);
                for (String line : lines) {
                    list.add(line);
                    System.out.println("line = " + line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
    }
}
