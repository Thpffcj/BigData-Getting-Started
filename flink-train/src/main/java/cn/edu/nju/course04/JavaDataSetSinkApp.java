package cn.edu.nju.course04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by thpffcj on 2019-07-04.
 */
public class JavaDataSetSinkApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Integer> info = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            info.add(i);
        }
        DataSource<Integer> data = env.fromCollection(info);

        String filePath = "file:///Users/thpffcj/Public/data/sink-out";

        data.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);

        env.execute("JavaDataSetSinkApp");
    }
}
