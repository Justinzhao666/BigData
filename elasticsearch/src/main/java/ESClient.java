import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ESClient {

    //region 创建索引数据等
    private TransportClient client;

    /**
     * 获取连接客户端
     */
    @Before
    public void getClient() throws UnknownHostException {

        // 设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 连接集群
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress((new TransportAddress(InetAddress.getByName("node1"), 9300)));

        System.out.println(client.toString());
    }

    /**
     * 创建索引
     */
    @Test
    public void createIndex() {
        // indices 是所有的索引值
        client.admin().indices().prepareCreate("book").get();
        client.close();
    }

    /**
     * 删除索引
     */
    @Test
    public void deleteIndex() {
        client.admin().indices().prepareDelete("book").get();
        client.close();
    }

    /**
     * 新建文档
     */
    @Test
    public void insertDocByJSON() {
        Book book = new Book("三体", 1000, "刘慈欣");
        String doc = JSON.toJSONString(book);
        System.out.println(doc);
        //创建文档
        IndexResponse indexResponse = client.prepareIndex("book", "basic", "1")
                .setSource(doc, XContentType.JSON).get();
        //打印返回结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        //关闭连接
        client.close();
    }

    /**
     * 使用map的方式将输入放入到es中
     * 还有其他的一些put方式可以看对应版本的文档：
     * https://www.elastic.co/guide/en/elasticsearch/client/java-api/6.4/java-docs-index.html
     */
    @Test
    public void insertDocByMap() {
        Map<String, Object> bookMap = new HashMap<>();
        bookMap.put("name", "红楼梦");
        bookMap.put("page", 10000);
        bookMap.put("author", "曹雪芹");
        //创建文档
        IndexResponse indexResponse = client.prepareIndex("book", "basic", "2")
                .setSource(bookMap)
                .execute()
                .actionGet();
        //打印返回结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        //关闭连接
        client.close();
    }
    //endregion

    //region 查询

    /**
     * 单个索引的查询
     */
    @Test
    public void queryIndex() {
        // 这里直接是按照id来获取数据
        GetResponse getResponse = client.prepareGet("book", "basic", "1").get();
        System.out.println(getResponse.getSourceAsString());
        client.close();
    }

    /**
     * 查询多个索引
     */
    @Test
    public void queryMultiIndex() {
        MultiGetResponse responses = client.prepareMultiGet().add("book", "basic", "1")
                .add("book", "basic", "1", "2").get();
        for (MultiGetItemResponse response : responses) {
            GetResponse data = response.getResponse();
            if (data.isExists()) {
                System.out.println(data.getSourceAsString());
            }
        }

    }
    //endregion

    //region 删改

    /**
     * 更新文档
     */
    @Test
    public void updateDoc() throws IOException, ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("book");
        updateRequest.type("basic");
        updateRequest.id("1");

        updateRequest.doc(
                XContentFactory.jsonBuilder().startObject()
                        .field("name", "三体2")
                        .field("author", "老刘")
                        .field("create_date", "2019-8-8")
                        .endObject());
        // 获取更新后的值
        UpdateResponse indexResponse = client.update(updateRequest).get();
        // 打印返回的结果
        System.out.println(indexResponse.toString());
        // 关闭连接
        client.close();
    }

    /**
     * 先查询有没有该id的数据，如果有就更新，
     */
    @Test
    public void upsertDoc() throws IOException, ExecutionException, InterruptedException {
        // 设置查询条件, 查找不到则添加
        IndexRequest request = new IndexRequest("book", "basic", "1")
                .source(XContentFactory.jsonBuilder().startObject()
                        .field("name", "三体3")
                        .endObject());

        // 设置更新, 查找到更新下面的设置 todo:有没有感觉这种写法很蠢？
        UpdateRequest upsert = new UpdateRequest("book", "basic", "1")
                .doc(XContentFactory.jsonBuilder().startObject().field("name", "西游记").endObject()).upsert(request);

        client.update(upsert).get();
        client.close();

    }

    /**
     * 删除文档
     */
    @Test
    public void deleteDoc() {
        DeleteResponse indexResponse = client.prepareDelete("book", "basic", "1").get();
        client.close();
    }
    //endregion

}

@Data
class Book {

    private String name;
    private Integer page;
    private String author;

    public Book(String name, Integer page, String author) {
        this.name = name;
        this.page = page;
        this.author = author;
    }
}