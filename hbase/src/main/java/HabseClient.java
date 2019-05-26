import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HabseClient {

    private static Connection connection;

    static {
        try {
            connection = getNewConnectApi();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static HBaseAdmin getOldConnectApi() throws IOException {
        // 创建配置文件
        HBaseConfiguration configuration = new HBaseConfiguration();
        configuration.set("hbase.zookeeper.quorum", "192.168.1.200");
        // 获取Hbase的管理对象
        return new HBaseAdmin(configuration);
    }

    private static Connection getNewConnectApi() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.1.200");
        Connection conn = ConnectionFactory.createConnection(configuration);
        return conn;
    }


    /**
     * 表是否存在
     */
    public boolean tableExist(String tableName) throws IOException {
        HBaseAdmin admin = getOldConnectApi();
        boolean exist = admin.tableExists(tableName);
        // 关闭资源
        admin.close();
        return exist;
    }

    public boolean tableExistNew(String tableName) throws IOException {
        Admin admin = getNewConnectApi().getAdmin();
        boolean exist = admin.tableExists(TableName.valueOf(tableName));
        // 关闭资源
        admin.close();
        return exist;
    }

    /**
     * 创建表
     */
    public void createTale(String tableName, String... columnFamilies) throws IOException {
        if (tableExistNew(tableName)) {
            System.out.println(tableName + " table exist!");
            return;
        }
        Admin admin = getNewConnectApi().getAdmin();
        // 需要表描述器
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        // 添加列族信息
        for (String cf : columnFamilies) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
            //columnDescriptor.setMaxVersions(3); 设置版本限制
            tableDescriptor.addFamily(columnDescriptor);
        }
        admin.createTable(tableDescriptor);
        System.out.println(tableName + " table created!");
        admin.close();
    }

    /**
     * 添加数据
     * 这里只是增加了单个，包括列族中的列也是单个，如果多个可以循环调用，然后计数达到某个值再插入到 Hbase （生产环境可以这么做）
     * 或者封个单个方法可以添加所有的列族下的列
     */
    public void insert(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        // 获取到表对象
        // 需要走connection而不是admin
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 设置put对象，初始化rowkey
        Put put = new Put(Bytes.toBytes(rowKey));
        // 设置具体数据
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        // 添加操作
        table.put(put);
        table.close();
    }

    public void scanTable(String tName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ",ColFamily:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ",Col:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ",Value:" + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }
        table.close();
    }
}
