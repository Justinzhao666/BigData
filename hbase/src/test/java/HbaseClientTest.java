import curd.HbaseClient;
import org.junit.Test;

import java.io.IOException;

public class HbaseClientTest {

    private static HbaseClient client = new HbaseClient();

    @Test
    public void testTableExist() throws IOException {
        System.out.println("staff = " + client.tableExist("staff"));
        System.out.println("student =" + client.tableExist("student"));
    }

    @Test
    public void testTableExistNew() throws IOException {
        System.out.println("new api : staff = " + client.tableExistNew("staff"));
        System.out.println("new api : student =" + client.tableExistNew("student"));
    }

    @Test
    public void testCreateTale() throws IOException {
        client.createTale("person", "base", "expr", "rewards");
        System.out.println("person = " + client.tableExistNew("person"));
    }

    @Test
    public void testInsert() throws IOException {
        client.insert("person", "1000", "base", "name", "justin4");
    }

    @Test
    public void testScanTable() throws IOException {
        client.scanTable("person");
    }

    @Test
    public void testDeleteTable() throws Exception {
        client.deleteTable("person");
    }


    @Test
    public void testDeleteData() throws Exception {
        client.deleteData("person", "1000", "base", "name");
    }

    @Test
    public void testGetData() throws IOException {
        client.getData("person", "1000", "base", "name");
    }
}