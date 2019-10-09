package cn.edu.nju.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;


/**
 * Created by Thpffcj on 2018/1/3.
 */
public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://192.168.92.130:8020";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    /**
     * 创建HDFS目录
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        outputStream.write("hello hadoop".getBytes());
        outputStream.flush();
        outputStream.close();
    }

    /**
     * 查看HDFS文件的内容
     * @throws Exception
     */
    @Test
    public void cat() throws Exception {
        FSDataInputStream inputStream = fileSystem.open(new Path("/hello.txt"));
        IOUtils.copyBytes(inputStream, System.out, 1024);
        inputStream.close();
    }

    /**
     * 重命名
     * @throws Exception
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath, newPath);
    }

    /**
     * 上传文件到HDFS
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path localPath = new Path("D:/hadoop.txt");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    /**
     * 上传大文件到HDFS
     * @throws Exception
     */
    @Test
    public void copyFromLocalFileWithProgress() throws Exception {
        InputStream inputStream = new BufferedInputStream(
                new FileInputStream(new File("D:/java/jdk-8u151-windows-x64.exe")));

        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/test/jdk-8"),
                new Progressable() {
                    @Override
                    // 带进度提醒信息
                    public void progress() {
                        System.out.print(".");
                    }
                });

        IOUtils.copyBytes(inputStream, outputStream, 4096);
    }

    /**
     * 下载HDFS文件
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception {
        Path localPath = new Path("D:/hadoop1.txt");
        Path hdfsPath = new Path("/hdfsapi/test/hadoop.txt");
        fileSystem.copyToLocalFile(false, hdfsPath, localPath, true);
    }

    /**
     * 查看某个目录下的所有文件
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsapi/test"));

        for(FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();

            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    /**
     * 删除
     * @throws Exception
     */
    @Test
    public void delete() throws Exception{
        fileSystem.delete(new Path("/hdfsapi/test"), true);
    }

    @Before
    public void setUp() throws Exception {
        System.out.println("HDFSApp.setUp");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "root");
    }

    @After
    public void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;
        System.out.println("HDFSApp.tearDown");
    }
}
