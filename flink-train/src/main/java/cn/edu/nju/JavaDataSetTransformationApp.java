package cn.edu.nju;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by thpffcj on 2019-07-02.
 */
public class JavaDataSetTransformationApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        mapFunction(env);
//        mapPartitionFunction(env);
//        firstFunction(env);
//        flatMapFunction(env);
//        distinctFunction(env);
//        joinFunction(env);
//        outJoinFunction(env);
        crossFunction(env);
    }

    public static void crossFunction(ExecutionEnvironment env) throws Exception {
        List<String> info1 = new ArrayList();
        info1.add("曼联");
        info1.add("曼城");

        List<String> info2 = new ArrayList();
        info2.add("3");
        info2.add("1");
        info2.add("0");

        DataSource<String> data1 = env.fromCollection(info1);
        DataSource<String> data2 = env.fromCollection(info2);

        data1.cross(data2).print();
    }

    public static void outJoinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList();
        info1.add(new Tuple2(1, "Thpffcj1"));
        info1.add(new Tuple2(2, "Thpffcj2"));
        info1.add(new Tuple2(3, "Thpffcj3"));
        info1.add(new Tuple2(4, "Thpffcj4"));

        List<Tuple2<Integer, String>> info2 = new ArrayList();
        info2.add(new Tuple2(1, "南京"));
        info2.add(new Tuple2(2, "北京"));
        info2.add(new Tuple2(3, "上海"));
        info2.add(new Tuple2(5, "成都"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        data1.fullOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (first == null) {
                    return new Tuple3<>(second.f0, "-", second.f1);
                } else if (second == null) {
                    return new Tuple3<>(first.f0, first.f1, "-");
                } else {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                }
            }
        }).print();
    }

    public static void joinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList();
        info1.add(new Tuple2(1, "Thpffcj1"));
        info1.add(new Tuple2(2, "Thpffcj2"));
        info1.add(new Tuple2(3, "Thpffcj3"));
        info1.add(new Tuple2(4, "Thpffcj4"));

        List<Tuple2<Integer, String>> info2 = new ArrayList();
        info2.add(new Tuple2(1, "南京"));
        info2.add(new Tuple2(2, "北京"));
        info2.add(new Tuple2(3, "上海"));
        info2.add(new Tuple2(5, "成都"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        data1.join(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                return new Tuple3<>(first.f0, first.f1, second.f1);
            }
        }).print();
    }

    public static void distinctFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList<>();
        info.add("hadoop,spark");
        info.add("hadoop,flink");
        info.add("flink,flink");

        DataSource<String> data = env.fromCollection(info);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String input, Collector<String> out) throws Exception {
                String[] splits = input.split(",");
                for (String split : splits) {
                    out.collect(split);
                }
            }
        }).distinct().print();
    }

    public static void flatMapFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList<>();
        info.add("hadoop,spark");
        info.add("hadoop,flink");
        info.add("flink,flink");

        DataSource<String> data = env.fromCollection(info);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String input, Collector<String> out) throws Exception {
                String[] splits = input.split(",");
                for (String split : splits) {
                    out.collect(split);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<String, Integer>(value, 1);
            }
        }).groupBy(0).sum(1).print();
    }

    public static void firstFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info = new ArrayList();
        info.add(new Tuple2(1, "Hadoop"));
        info.add(new Tuple2(1, "Spark"));
        info.add(new Tuple2(1, "Flink"));
        info.add(new Tuple2(2, "Java"));
        info.add(new Tuple2(2, "Spring Boot"));
        info.add(new Tuple2(3, "Linux"));
        info.add(new Tuple2(4, "Vue"));

        DataSource<Tuple2<Integer, String>> data = env.fromCollection(info);

        data.first(3).print();
        data.groupBy(0).first(2).print();
        data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();
    }

    public static void mapPartitionFunction(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            list.add("student: " + i);
        }
        DataSource<String> data = env.fromCollection(list);

        data.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> inputs, Collector<String> out) throws Exception {
                String connection = DBUtils.getConnection();
                System.out.println("connection = " + connection);
                DBUtils.returnConnection(connection);
            }
        }).print();
    }

    public static void filterFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        DataSource<Integer> data = env.fromCollection(list);

        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer input) throws Exception {
                return input + 1;
            }
        }).filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer input) throws Exception {
                return input > 5;
            }
        }).print();
    }

    public static void mapFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        DataSource<Integer> data = env.fromCollection(list);

        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer input) throws Exception {
                return input + 1;
            }
        }).print();
    }
}
