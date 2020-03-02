package cn.edu.nju.hadoop.mapreduce.sort;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by thpffcj on 2020/2/12.
 */
public class SecondarySort {

    public static class Map extends Mapper<LongWritable, Text, IntPair, IntWritable> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            int left = 0;
            int right = 0;
            if (tokenizer.hasMoreTokens()) {
                left = Integer.parseInt(tokenizer.nextToken());
                if (tokenizer.hasMoreTokens()) {
                    right = Integer.parseInt(tokenizer.nextToken());
                }
                context.write(new IntPair(left, right), new IntWritable(right));
            }
        }
    }

    /*
     * 自定义分区函数类FirstPartitioner，根据 IntPair中的first实现分区
     */
    public static class FirstPartitioner extends Partitioner<IntPair, IntWritable>{
        @Override
        public int getPartition(IntPair key, IntWritable value, int numPartitions){
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

    /*
     * 自定义GroupingComparator类，实现分区内的数据分组
     */
    public static class GroupingComparator extends WritableComparator{

        protected GroupingComparator(){
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            int l = ip1.getFirst();
            int r = ip2.getFirst();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
    }

    public static class Reduce extends Reducer<IntPair, IntWritable, Text, IntWritable> {

        public void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(new Text(Integer.toString(key.getFirst())), val);
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 读取配置文件
        Configuration conf = new Configuration();

        // 判断路径是否存在，如果存在，则删除
        Path mypath = new Path(args[1]);
        FileSystem hdfs = mypath.getFileSystem(conf);
        if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }

        Job job = new Job(conf, "secondarysort");
        // 设置主类
        job.setJarByClass(SecondarySort.class);

        // 输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Mapper
        job.setMapperClass(Map.class);
        // Reducer
        job.setReducerClass(Reduce.class);

        // 分区函数
        job.setPartitionerClass(FirstPartitioner.class);

        // 本示例并没有自定义SortComparator，而是使用IntPair中compareTo方法进行排序 job.setSortComparatorClass();
        // 分组函数
        job.setGroupingComparatorClass(GroupingComparator.class);

        // map输出key类型
        job.setMapOutputKeyClass(IntPair.class);
        // map输出value类型
        job.setMapOutputValueClass(IntWritable.class);

        // reduce输出key类型
        job.setOutputKeyClass(Text.class);
        // reduce输出value类型
        job.setOutputValueClass(IntWritable.class);

        // 输入格式
        job.setInputFormatClass(TextInputFormat.class);
        // 输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
