package cn.edu.nju.hadoop.mapreduce.topk;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by thpffcj on 2020/2/5.
 */
public class IPTimes implements WritableComparable {

    // IP
    private Text ip;
    // IP对应出现的次数
    private IntWritable count;

    // 无参构造函数(一定要有，反射机制会出错，另外要对定义的变量进行初始化否则会报空指针异常)
    public IPTimes() {
        this.ip = new Text("");
        this.count = new IntWritable(1);
    }

    // 有参构造函数
    public IPTimes(Text ip, IntWritable count) {
        this.ip = ip;
        this.count = count;
    }

    // 反序列化
    public void readFields(DataInput in) throws IOException {
        ip.readFields(in);
        count.readFields(in);
    }

    // 序列化
    public void write(DataOutput out) throws IOException {
        ip.write(out);
        count.write(out);
    }

    public Text getIp() {
        return ip;
    }

    public void setIp(Text ip) {
        this.ip = ip;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }

    // 这个方法是二次排序的关键
    public int compareTo(Object o) {
        // 强转
        IPTimes ipAndCount = (IPTimes) o;
        // 对第二列的count进行比较
        long minus = this.getCount().compareTo(ipAndCount.getCount());
        // 第二列不相同时降序排列
        if (minus != 0) {
            return ipAndCount.getCount().compareTo(this.count);
        } else {  // 第二列相同时第一列升序排列
            return this.ip.compareTo(ipAndCount.getIp());
        }
    }

    // hashCode和equals()方法
    public int hashCode() {
        return ip.hashCode();
    }

    public boolean equals(Object o) {
        if (!(o instanceof IPTimes)) {
            return false;
        }
        IPTimes other = (IPTimes) o;
        return ip.equals(other.ip) && count.equals(other.count);
    }

    // 重写toString()方法
    public String toString() {
        return this.ip + "\t" + this.count;
    }
}
