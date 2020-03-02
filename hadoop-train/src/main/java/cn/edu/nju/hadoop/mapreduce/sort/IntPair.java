package cn.edu.nju.hadoop.mapreduce.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Created by thpffcj on 2020/2/12.
 *
 * 自定义key排序
 * 在mr中，所有的key是需要被比较和排序的，并且是二次，先根据partitioner，再根据大小。而本例中也是要比较两次。
 * 先按照第一字段排序，然后再对第一字段相同的按照第二字段排序。
 * 根据这一点，我们可以构造一个复合类IntPair，他有两个字段，先利用分区对第一字段排序，再利用分区内的比较对第二字段排序
 */
public class IntPair implements WritableComparable<IntPair> {

    int first;
    int second;

    public IntPair(){
    }

    public IntPair(int first, int second){
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    // 反序列化，从流中读进二进制转换成IntPair
    @Override
    public void readFields(DataInput in) throws IOException {
        this.first = in.readInt();
        this.second = in.readInt();
    }

    // 序列化，将IntPair转换成二进制输出
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    /*
     * 为什么要重写equal方法？
     * 因为Object的equal方法默认是两个对象的引用的比较，意思就是指向同一内存,地址则相等，否则不相等；
     * 如果你现在需要利用对象里面的值来判断是否相等，则重载equal方法。
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (obj instanceof IntPair) {
            IntPair r = (IntPair) obj;
            return r.first == first && r.second == second;
        } else {
            return false;
        }
    }

    /*
     * 重写equal 的同时为什么必须重写hashcode？
     * hashCode是编译器为不同对象产生的不同整数，根据equal方法的定义：如果两个对象是相等（equal）的，那么两个对象
     * 调用 hashCode必须产生相同的整数结果
     * 即：equal为true，hashCode必须为true，equal为false，hashCode也必须 为false，所以必须重写hashCode来保证
     * 与equal同步。
     */
    @Override
    public int hashCode() {
        return first * 157 + second;
    }

    // 实现key的比较
    @Override
    public int compareTo(IntPair o) {
        if (first != o.first) {
            return first < o.first ? -1 : 1;
        } else if (second != o.second) {
            return second < o.second ? -1 : 1;
        } else {
            return 0;
        }
    }
}
