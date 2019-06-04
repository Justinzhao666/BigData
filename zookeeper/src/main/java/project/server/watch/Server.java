package project.server.watch;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * 模拟一下这是一个分布式服务器
 * 服务器想zk注册自己：创建一个节点：采用的是临时+序号的方式创建节点：这样可以自动为节点分配序号
 */
public class Server {

    public static void main(String[] args) throws Exception {
        Server server = new Server();
        //连接zk服务器
        server.getConn();
        //注册写入服务器节点
        server.getRegister(args[0]);
        //模拟服务器的业务
        server.severService();
    }


    private String connStr = "node1:2181,node2:2181,node3:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    private void getConn() throws IOException {
        zkClient = new ZooKeeper(connStr, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    private void getRegister(String data) throws KeeperException, InterruptedException {
        String path = zkClient.create("/servers/server", data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(data + " is online : create node :" + path);
    }

    private void severService() throws InterruptedException {
        //模拟服务器自己的一些业务操作
        Thread.sleep(Long.MAX_VALUE);
    }

}
