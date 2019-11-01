
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.*;

public class HiveTest {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://rhel1:10000/default;principal=hive/rhel1@GEMS.COM";
    private static String sql = "select * from table34122 limit 1";

    private static ResultSet res;
    public static void main(String[] args) {
        Connection connection = null;
        Statement stat = null;
        try{
            connection = getConnection();
            stat = connection.createStatement();
            res = stat.executeQuery(sql);
            while (res.next())
            {
                System.out.println(res.getString(1));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try{
                if(stat != null)
                {
                    stat.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try{
                if(connection != null)
                {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public static Connection getConnection()
    {
        System.setProperty("java.security.krb5.conf","conf/krb5.conf");
        Configuration conf = new Configuration();
        conf.setBoolean("hadoop.security.authorization",true);
        conf.set("hadoop.security.authentication", "Kerberos");

        UserGroupInformation.setConfiguration(conf);
        Connection connection = null;
        try{
            UserGroupInformation.loginUserFromKeytab("hive@GEMS.COM","conf/hive.keytab");
            Class.forName(driverName);
            connection = DriverManager.getConnection(url);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return connection;


    }
}
