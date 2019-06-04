package project.server.watch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 客户端：
 * 主要监听zk的server节点下的子节点信息
 * <p>
 * 测试方法：
 * 启动Client：然后到zk上手动添加服务器节点，查看Client的日志
 * create -e -s /servers/server nodeX
 */
public class Client {

    public static void main(String[] args) throws Exception {
        Client client = new Client();
        // 客户端获取连接
        client.getConn();
        // 客户端开启监听 - 服务器节点的更新
        client.getChild();
        // 客户端自己业务
        client.clientService();
    }


    private String connStr = "node1:2181,node2:2181,node3:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;


    private void getConn() throws IOException {
        zkClient = new ZooKeeper(connStr, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    // 只要节点发生了更新就再次watch一下更新
                    getChild();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void getChild() throws Exception {
        List<String> childes = zkClient.getChildren("/servers", true);
        // 所有的在线主机
        List<String> servers = new ArrayList<>();
        for (String child : childes) {
            //获取节点的数据
            byte[] data = zkClient.getData("/servers/" + child, false, null);
            servers.add(new String(data));
        }
        servers.forEach(System.out::println);
    }

    private void clientService() throws InterruptedException {
        //模拟服务器自己的一些业务操作
        Thread.sleep(Long.MAX_VALUE);
    }
}
