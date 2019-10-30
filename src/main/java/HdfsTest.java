import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class HdfsTest {
    public static void main(String[] args) throws IOException {
        System.setProperty("java.security.krb5.conf","conf/krb5.conf");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel2:8020");
        conf.setBoolean("hadoop.security.authorization",true);
        conf.set("hadoop.security.authentication","Kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("hdfs@GEMS.COM","conf/hdfs.keytab");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://rhel2:8020/test");

        if (fs.exists(path))
        {
            System.out.println("===contains===");
        }
    }
}
