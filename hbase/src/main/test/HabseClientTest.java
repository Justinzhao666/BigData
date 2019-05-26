import org.junit.Test;

import java.io.IOException;

public class HabseClientTest {

    private static HabseClient client = new HabseClient();

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
        client.insert("person", "1000", "base", "name", "justin");
    }

    @Test
    public void testScanTable() throws IOException {
        client.scanTable("person");
    }
}