package cn.edu.nju.hadoop.mapreduce.sort;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by thpffcj on 2020/2/5.
 * <p>
 * 让MapReduce产生一个全局排序的文件：
 * <p>
 * 1. 最简单的方法是只使用一个分区(partition)，这种在处理小规模文件时还行。但是在处理大型文件是效率极低，所有的数据都发送到一
 * 个Reduce进行排序，这样不能充分利用集群的计算资源，而且在数据量很大的情况下，很有可能会出现OOM问题。
 * 2. 首先创建一系列排好序的文件，其次串联这些文件，最后生成一个全局排序的文件。它主要的思路使用一个partitioner来描述输出的全
 * 局排序。该方案的重点在于分区方法，默认情况下根据hash值进行分区(默认的分区函数是HashPartitioner，其实现的原理是计算map输
 * 出key的 hashCode ，然后对Reduce个数 求余，余数相同的 key 都会发送到同一个Reduce)；还可以根据用户自定义partitioner
 * (自定义一个类并且继承partitioner类，重写器getpartition方法)
 */
class GlobalSortPartitioner extends Partitioner<Text, LongWritable> implements Configurable {

    private Configuration configuration = null;
    private int indexRange = 0;

    public int getPartition(Text text, LongWritable longWritable, int numPartitions) {
        // 假如取值范围等于26的话，那么就意味着只需要根据第一个字母来划分索引
        int index = 0;
        if (indexRange == 26) {
            index = text.toString().toCharArray()[0] - 'a';
        } else if (indexRange == 26 * 26) {
            //这里就是需要根据前两个字母进行划分索引了
            char[] chars = text.toString().toCharArray();
            if (chars.length == 1) {
                index = (chars[0] - 'a') * 26;
            }
            index = (chars[0] - 'a') * 26 + (chars[1] - 'a');
        }
        int perReducerCount = indexRange / numPartitions;
        if (indexRange < numPartitions) {
            return numPartitions;
        }

        for (int i = 0; i < numPartitions; i++) {
            int min = i * perReducerCount;
            int max = (i + 1) * perReducerCount - 1;
            if (index >= min && index <= max) {
                return i;
            }
        }
        //这里我们采用的是第一种不太科学的方法
        return numPartitions - 1;
    }

    public void setConf(Configuration conf) {
        this.configuration = conf;
        indexRange = configuration.getInt("key.indexRange", 26 * 26);
    }

    public Configuration getConf() {
        return configuration;
    }
}