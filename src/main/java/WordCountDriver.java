import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;


public class WordCountDriver {
    public static void main(String[] args)throws Exception {
        System.setProperty("java.security.krb5.conf","/etc/krb5.conf");
        Configuration conf = new Configuration();

        conf.set("mapreduce.framework.name", "yarn");

		conf.set("yarn.resourcemanager.hostname", "rhel2");
		conf.set("fs.defaultFS", "hdfs://rhel2:8020/");
        conf.setBoolean("hadoop.security.authorization",true);
        conf.set("hadoop.security.authentication","Kerberos");
		conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("hdfs@GEMS.COM","/opt/keytabs/hdfs.keytab");
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountDriver.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path("hdfs://rhel2:8020/test/sparkdata/wc.txt"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://rhel2:8020/test/sparkdata/output2"));


        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);


    }
}
