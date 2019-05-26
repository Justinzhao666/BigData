import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        //core-site里面的配置项目
        conf.set("fs.defaultFS", "hdfs://node1:9000");
        //这种方式需要修改用户
        //FileSystem fs = FileSystem.get(conf);
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), conf, "root");
        // 创建一个目录
        fs.mkdirs(new Path("/0513/justin"));
        fs.close();
        System.out.println("done");
    }

    @Test
    public void copyFile() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:9000");
        conf.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), conf, "root");
        fs.copyFromLocalFile(new Path("/Users/zhaohaoren/workspace/code/mine/JavaProjects/BigData/hdfs/src/main/resources/content"), new Path("/0513/justin/content3.txt"));
        fs.close();
    }

    @Test
    public void downFile() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:9000");
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), conf, "root");
        fs.copyToLocalFile(new Path("/0513/justin/content3.txt"), new Path("down.txt"));
        // 还有一个4个参数的
        fs.copyToLocalFile(false, new Path("/0513/justin/content3.txt"), new Path("down2.txt"), true);
        // 使用本地模式不会生成crc校验文件
        fs.close();
    }

    @Test
    public void delete() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:9000");
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), conf, "root");
        fs.delete(new Path("/0513/justin/content3.txt"), false);
        fs.close();
    }

    @Test
    public void list() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:9000");
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), conf, "root");
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/0513"), true);
        while (iter.hasNext()) {
            LocatedFileStatus next = iter.next();
            System.out.println(next.getPath().getName());
            System.out.println(next.getPermission());
            System.out.println(next.getLen());
            BlockLocation[] blockLocations = next.getBlockLocations();
            for (BlockLocation location : blockLocations) {
                System.out.println(JSON.toString(location.getHosts()));
            }
            System.out.println("----------------------");
        }

        fs.close();
    }

    // 文件改名等略过

    // IO 流操作-上传
    @Test
    public void io() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:9000");
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), conf, "root");
        //先要获取一个输入流
        FileInputStream fis = new FileInputStream(new File("/Users/zhaohaoren/workspace/code/mine/JavaProjects/BigData/hdfs/src/main/resources/content"));
        //再获取一个输出流
        FSDataOutputStream fos = fs.create(new Path("/io.txt"));
        IOUtils.copyBytes(fis, fos, conf);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }


    // 分块读取hadoop的大文件
    @Test
    public void seek1() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:9000");
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), conf, "root");
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.7.tar.gz"));
        FileOutputStream fos = new FileOutputStream(new File("hadoop-2.7.7.tar.gz.part1"));

        //这里开始只读取部分
        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(buf);
            fos.write(buf);
        }
        fs.close();
    }

    @Test
    public void seek2() throws Exception {
        // 继续读第二块
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:9000");
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), conf, "root");
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.7.tar.gz"));
        fis.seek(1024 * 1024 * 128); // 定位到要读取的位置 重点就是这个方法。
        FileOutputStream fos = new FileOutputStream(new File("hadoop-2.7.7.tar.gz.part2"));
        IOUtils.copyBytes(fis, fos, conf);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

}
