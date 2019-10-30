

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.sql.*;

public class ImpalaTest {
    private static String connectionUrl = "jdbc:impala://rhel3:21050/hbase_impala;AuthMech=1;KrbRealm=GEMS.COM;KrbHostFQDN=rhel3;KrbServiceName=impala";
    private static String jdbcDriverName = "com.cloudera.impala.jdbc41.Driver";


    public static void main(String[] args) throws IOException, SQLException {
        System.setProperty("java.security.krb5.conf","conf/krb5.conf");
        Configuration configuration = new Configuration();
        configuration.set("hadoop.security.authentication","Kerberos");
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab("impala@GEMS.COM","conf/impala.keytab");

        String sqlStatement = "select count(*) from hive_ccjcxx";
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;

        try{
            connection  = getConnection();
            System.out.println("获取连接成功");
            ps = connection.prepareStatement(sqlStatement);
             rs = ps.executeQuery();
            while(rs.next())
            {
                System.out.println(rs.getString(1));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(connection!=null)
            {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
            if(ps!=null)
            {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(rs!=null)
            {
                rs.close();
            }

        }

    }

    public static  Connection getConnection() throws IOException {

        Object o = UserGroupInformation.getLoginUser().doAs(
                new PrivilegedAction<Object>() {
                    public Object run()
                    {
                        Connection connection = null;
                        try{
                            Class.forName(jdbcDriverName);
                            connection = DriverManager.getConnection(connectionUrl);
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                        return  connection;
                    }

                }
        );

        return (Connection) o;


    }
}
