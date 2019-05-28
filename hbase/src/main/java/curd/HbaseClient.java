package curd;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HbaseClient {

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
        configuration.set("hbase.zookeeper.quorum", "node1");
        // 获取Hbase的管理对象
        return new HBaseAdmin(configuration);
    }

    private static Connection getNewConnectApi() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node1");
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
        // 这里在创建表的时候还可以设置其预分区规则 byte[][] splitKeys
        // 为什么是二维数组：
        // HBase里面数据都是Byte[]数组，我们指定的splitKeys，比如 1000,2000,3000 这里面的1000也是一个bytes数组。所以是byte[][]

        System.out.println(tableName + " table created!");
        admin.close();
    }

    /**
     * 删除表
     */
    public void deleteTable(String tName) throws Exception {
        //删除表之前先使表不可以用
        Admin admin = connection.getAdmin();
        admin.disableTable(TableName.valueOf(tName));
        admin.deleteTable(TableName.valueOf(tName));
        System.out.println(tName + " table delete!");
        admin.close();
    }


    /**
     * 添加/修改数据
     * 这里只是增加了单个，包括列族中的列也是单个，如果多个可以循环调用，然后计数达到某个值再插入到 Hbase （生产环境可以这么做）
     * 或者封个单个方法可以添加所有的列族下的列
     */
    public void insert(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        // 获取到表对象
        // 需要走connection而不是admin
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 设置put对象，初始化row key
        Put put = new Put(Bytes.toBytes(rowKey));
        // 设置具体数据
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        // 添加操作
        table.put(put);
        table.close();
    }

    /**
     * 删除数据
     */
    public void deleteData(String tName, String rowKey, String cf, String cn) throws IOException {
        // 获取table对象
        Table table = connection.getTable(TableName.valueOf(tName));
        // 创建delete对象
        // 删除整个rowKey == deleteall操作
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        if (StringUtils.isNotBlank(cf)) {
            // 删除列族
            delete.addFamily(Bytes.toBytes(cf));
            if (StringUtils.isNotBlank(cn)) {
                // 删除指定列所有版本
                delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
                // 删除最新版本，还可以传个版本  【该操作会先获取最新版本，然后添加一个删除标记】
                // -- 这个慎用，他删除数据不会把数据删掉,只是标记最新那个为删除，查询的时候则会显示老版本的数据
                // delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
            }

        }
        table.delete(delete);
        table.close();
    }

    /**
     * scan表
     */
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

    /**
     *
     */
    public void getData(String tName, String rowKey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tName));
        // 创建一个get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
         get.setMaxVersions(3);
        // 获取数据
        Result result = table.get(get);
        // 显示数据
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            System.out.println("Get#RowKey:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                    ",ColFamily:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    ",Col:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    ",Value:" + Bytes.toString(CellUtil.cloneValue(cell)) +
                    ",Timestamp:"+ cell.getTimestamp()
            );
        }
        table.close();
    }
}
