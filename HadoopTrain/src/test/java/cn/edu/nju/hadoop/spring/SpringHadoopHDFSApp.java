package cn.edu.nju.hadoop.spring;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

/**
 * Created by Thpffcj on 2018/1/8.
 */
public class SpringHadoopHDFSApp {

    private ApplicationContext context;
    private FileSystem fileSystem;

    /**
     * 创建HDFS文件夹
     * @throws Exception
     */
    @Test
    public void testMkdirs() throws Exception {
        fileSystem.mkdirs(new Path("/springhdfs/"));
    }

    /**
     * 读取HDFS文件内容
     * @throws Exception
     */
    @Test
    public void testText() throws Exception {
        FSDataInputStream inputStream = fileSystem.open(new Path("/springhdfs/hello.txt"));
        IOUtils.copyBytes(inputStream, System.out, 1024);
        inputStream.close();
    }

    @Before
    public void setUp() {
        context = new ClassPathXmlApplicationContext("beans.xml");
        fileSystem = (FileSystem) context.getBean("fileSystem");
    }

    @After
    public void tearDown() throws IOException {
        context = null;
        fileSystem = null;
    }
}
