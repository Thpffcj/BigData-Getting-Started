package cn.edu.nju.hadoop.mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

/**
 * Created by thpffcj on 2020/2/5.
 */
public class GlobalSort {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //access hdfs's user
        System.setProperty("HADOOP_USER_NAME","root");

        Configuration conf = new Configuration();
        conf.set("mapred.jar", "D:\\MyDemo\\MapReduce\\Sort\\out\\artifacts\\TotalSort\\TotalSort.jar");

        FileSystem fs = FileSystem.get(conf);

        /*RandomSampler 参数说明
         * @param freq Probability with which a key will be chosen.
         * @param numSamples Total number of samples to obtain from all selected splits.
         * @param maxSplitsSampled The maximum number of splits to examine.
         */
        InputSampler.RandomSampler<Text, Text> sampler = new InputSampler.RandomSampler<>
                (0.1, 10, 10);

        //设置分区文件, TotalOrderPartitioner必须指定分区文件
        Path partitionFile = new Path( "_partitions");
        TotalOrderPartitioner.setPartitionFile(conf, partitionFile);

        Job job = Job.getInstance(conf);
        job.setJarByClass(GlobalSort.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class); //数据文件默认以\t分割
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(4);  //设置reduce任务个数，分区文件以reduce个数为基准，拆分成n段

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(TotalOrderPartitioner.class);

        FileInputFormat.addInputPath(job, new Path("/test/sort"));

        Path path = new Path("/test/wc/output");

        if(fs.exists(path))//如果目录存在，则删除目录
        {
            fs.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job, path);

        //将随机抽样数据写入分区文件
        InputSampler.writePartitionFile(job, sampler);

        boolean b = job.waitForCompletion(true);
        if(b) {
            System.out.println("OK");
        }
    }
}
