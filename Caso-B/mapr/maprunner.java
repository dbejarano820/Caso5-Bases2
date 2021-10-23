package mapr;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import mapers.BudgetMaper;
import reducers.BudgetReducer;

public class maprunner {
    public static void main(String[] args) throws IOException{
        JobConf conf = new JobConf(mapr.maprunner.class);
        conf.setJobName("Caso5 - A");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(FloatWritable.class);
        conf.setMapperClass(BudgetMaper.class);
        conf.setReducerClass(BudgetReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileInputFormat.setInputPaths(conf,new Path("/datainput/PJCROD_PRESUPUESTO_V1.csv"));
        FileOutputFormat.setOutputPath(conf,new Path("/dataoutput"));
        JobClient.runJob(conf);
    }
}
