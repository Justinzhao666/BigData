package project.weibo.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import project.weibo.constant.Constant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WeiBoUtil {

    private static Configuration configuration = HBaseConfiguration.create();

    static {
        configuration.set(Constant.ZK_CONFIG_NAME, "node1");
    }

    /**
     * 创建命名空间 -- 数据库的库名
     */
    public static void createNamespace(String nameSpace) throws IOException {
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();
        //创建Namespace描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        //创建namespace
        admin.createNamespace(namespaceDescriptor);
        //关闭资源
        admin.close();
        conn.close();
    }

    /**
     * 创建表
     */
    public static void createTable(String tableName, Integer version, String... colummFamilis) throws IOException {
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();

        // 创建表描述器
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        // 设置列族
        for (String family : colummFamilis) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            columnDescriptor.setMaxVersions(version); // 这里设置version表示让HBase存多少个版本的数据，如果存的大于这个版本的就会直接被覆盖。
            tableDescriptor.addFamily(columnDescriptor);
        }
        admin.createTable(tableDescriptor);
        admin.close();
        conn.close();
    }

    /**
     * 发布微博：就是用户创建一个记录
     * 插入content表，用户发布的内容
     * 获取关系表，该用户的粉丝
     * 给该粉丝每个人都put一下该用户最新的动态
     */
    public static void createData(String uid, String content) throws IOException {
        Connection conn = ConnectionFactory.createConnection(configuration);

        Table tableContent = conn.getTable(TableName.valueOf(Constant.CONTENT));
        Table tableRelation = conn.getTable(TableName.valueOf(Constant.RELATIONS));
        Table tableInbox = conn.getTable(TableName.valueOf(Constant.INBOX));

        // 拼接RowKey
        long timestamp = System.currentTimeMillis();
        String rowKey = uid + "_" + timestamp;

        // 构建Put对象准备插入内容表
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(content));
        tableContent.put(put);

        // 获取所有的粉丝然后给他们发通知
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("fans"));
        Result result = tableRelation.get(get);
        Cell[] cells = result.rawCells();
        if (cells.length <= 0) {
            return;
        }
        // 获取到的所有粉丝
        List<Put> puts = new ArrayList<>();
        for (Cell cell : cells) {
            byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
            Put inboxPut = new Put(cloneQualifier);
            // 给每个粉丝在inbox消息表里面 对应的一行数据put该通知
            inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), Bytes.toBytes(rowKey));
            puts.add(inboxPut);
        }
        tableInbox.put(puts);

        tableContent.close();
        tableInbox.close();
        tableRelation.close();
        conn.close();
    }

    /**
     * 关注用户
     * relation表：该用户添加attends列表
     * 被关注的用户，各自添加当前用户为粉丝
     * inbox表中添加他关注的人的信息
     *
     * @param uid  操作用户
     * @param uids 关注了哪些人
     */
    public static void addAttend(String uid, String... uids) throws IOException {

        Connection conn = ConnectionFactory.createConnection(configuration);

        Table tableContent = conn.getTable(TableName.valueOf(Constant.CONTENT));
        Table tableRelation = conn.getTable(TableName.valueOf(Constant.RELATIONS));
        Table tableInbox = conn.getTable(TableName.valueOf(Constant.INBOX));

        // 为了添加当前用户的关注人
        Put relationPut = new Put(Bytes.toBytes(uid));
        List<Put> puts = new ArrayList<>();
        for (String s : uids) {
            // 对于该用户添加他的attends信息
            relationPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(s), Bytes.toBytes(s));
            // 对每一个粉丝添加他们的fans信息
            Put fanPut = new Put(Bytes.toBytes(s));
            fanPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
            puts.add(fanPut);
        }
        // puts为 添加粉丝的+ 添加关注的
        puts.add(relationPut);
        tableRelation.put(puts);


        Put inboxPut = new Put(Bytes.toBytes(uid));
        // 获取被关注者的数据(其实就是获取用户rowKey对应数据)
        // rowkey是uid+时间戳的，我们不知道rowkey是啥，所以：
        for (String s : uids) {
            // 使用扫描的方法：开始就是我们的uid，结束是uid+"|" 保证该uid发布的所有的信息都一定在我们扫描范围内： 因为我们使用uid_time,|>_所以能全包含。
            Scan scan = new Scan(Bytes.toBytes(s), Bytes.toBytes(s + "|"));
            ResultScanner scanner = tableContent.getScanner(scan);
            for (Result result : scanner) {
                String rowKey = Bytes.toString(result.getRow());
                String[] rows = rowKey.split("_");
                // 拿出的该用户的所有的content，需要放入inbox表中
                inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(s), Long.parseLong(rows[1]), result.getRow());
                //值就是row就是uid_timestamp
                //需要对其添加时间戳，因为关注用户发布的时候，也会创建一个时间戳那个时间戳是用户创建content的时间，所以这里也需要时那个content被创建出来的时间。
            }
        }
        tableInbox.put(inboxPut);

        tableContent.close();
        tableInbox.close();
        tableRelation.close();
        conn.close();
    }

    /**
     * 取关用户
     * 关系表删除操作者的关注用户
     * 删除关注用户的粉丝
     * inbox表删除操作者取关用户的信息
     */
    public static void deleteAttend(String uid, String... uids) throws IOException {
        Connection conn = ConnectionFactory.createConnection(configuration);
        Table tableRelation = conn.getTable(TableName.valueOf(Constant.RELATIONS));
        Table tableInbox = conn.getTable(TableName.valueOf(Constant.INBOX));
        // 删除操作者attend记录
        Delete delete = new Delete(Bytes.toBytes(uid));
        List<Delete> deletes = new ArrayList<>();
        for (String s : uids) {
            // 删除该用户的attend
            delete.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(s));
            // 删除取关用户的 fans
            Delete deleteFan = new Delete(Bytes.toBytes(s));
            deleteFan.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid));
            deletes.add(deleteFan);
        }
        deletes.add(delete);
        tableRelation.delete(deletes);
        // 删除inbox表的关注用户的记录
        Delete delInbox = new Delete(Bytes.toBytes(uid));
        for (String s : uids) {
            delInbox.addColumn(Bytes.toBytes("info"), Bytes.toBytes(s));
        }
        tableInbox.delete(delInbox);

        tableRelation.close();
        tableInbox.close();
        conn.close();
    }

    /**
     * 获取微博内容(获取某个人的所有微博)
     */
    public static void getPersonalWeiBo(String uid) throws IOException {
        Connection conn = ConnectionFactory.createConnection(configuration);
        Table tableContent = conn.getTable(TableName.valueOf(Constant.CONTENT));
        Scan scan = new Scan(Bytes.toBytes(uid));
        // 这里不使用范围去scan而是使用filter
        // 这些用户查看源码找对应的合适的参数，合适的实现类就行了
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));
        scan.setFilter(filter);
        ResultScanner scanner = tableContent.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        " Content:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    /**
     * 获取朋友圈
     * 这里我认为表设计是不科学的
     * 去inbox获取所有的关注者的rowKey
     * 去content去load数据
     */
    public static void getFriends(String uid) throws IOException {
        Connection conn = ConnectionFactory.createConnection(configuration);
        Table tableInbox = conn.getTable(TableName.valueOf(Constant.INBOX));
        Table tableContent = conn.getTable(TableName.valueOf(Constant.CONTENT));

        Get get = new Get(Bytes.toBytes(uid));
        get.setMaxVersions();//设置获取最大版本的数据,如果设置值就获取多个版本的数据
        Result result = tableInbox.get(get);
        Cell[] cells = result.rawCells();
        List<Get> gets = new ArrayList<>();
        for (Cell cell : cells) {
            Get contentGet = new Get(CellUtil.cloneValue(cell));
            gets.add(contentGet);
        }
        Result[] results = tableContent.get(gets);
        for (Result res : results) {
            Cell[] data = res.rawCells();
            for (Cell cell : data) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        " Content:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
}
