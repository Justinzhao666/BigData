import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZkClient {


    private String connStr = "node1:2181,node2:2181,node3:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    /**
     * 初始化连接
     */
    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connStr, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("nodes changed.....");
                try {
                    List<String> childes = zkClient.getChildren("/", true);
                    childes.forEach(System.out::println);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }


    /**
     * 创建节点
     */
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        /*
            创建的节点
            节点的数据-必填
            Ids: 访问权限的控制
            CreateMode：创建节点的类型：持久的还是临时的（带顺序还是不带顺序的）
         */
        String path = zkClient.create("/justin", "zhaohaoren".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }


    /**
     * 获取子节点并监听子节点数据的变化
     */
    @Test
    public void watchChildNodes() throws KeeperException, InterruptedException {
        /*
            true表示需要监听
            false表示不需要监听: 监听的逻辑在zk的创建连接的process函数里面。
         */
        List<String> childes = zkClient.getChildren("/", true);
        childes.forEach(System.out::println);
        // 为了操作添加结果可见 -- 睡眠3分钟
        Thread.sleep(1000 * 60 * 3);
    }

    /**
     * 判断节点是否存在
     */
    @Test
    public void nodeExist() throws KeeperException, InterruptedException {
        Stat exists = zkClient.exists("/justin", false);
        System.out.println(exists == null ? "no exist" : "exist");

    }
}
