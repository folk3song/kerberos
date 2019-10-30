import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class HbaseTest {
    public static void main(String[] args) throws IOException {
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf("hbase_test"));
        System.out.println(table.getName());
        getResult(table);
        putData(table);


    }


    public static Connection getConnection()
    {
        System.setProperty("java.security.krb5.conf","conf/krb5.conf");
        Configuration configuration = HBaseConfiguration.create();
        System.out.println(configuration.get("hbase.rootdir"));

        configuration.set("hadoop.security.authentication","Kerberos");
        Connection connection = null;
        try{
            UserGroupInformation.loginUserFromKeytab("hbase@GEMS.COM","conf/hbase.keytab");
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return  connection;

    }

    public static void  getResult(Table table) throws IOException {

        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs)
        {
            System.out.println(r.toString());
        }
    }

    public static  void putData(Table table) throws IOException {
        Put put = new Put("7".getBytes());
        put.addColumn("cf".getBytes(),"info".getBytes(),"jock".getBytes());
        table.put(put);
    }
}
