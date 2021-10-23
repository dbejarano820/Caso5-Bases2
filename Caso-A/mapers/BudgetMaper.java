package mapers;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class BudgetMaper extends MapReduceBase implements Mapper<LongWritable,Text,Text,ArrayWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text,ArrayWritable> output, Reporter reporter) throws IOException{
        String line = value.toString();
        System.out.println(line);

        String values[] = line.split(",");
        String division = values[2];
        String rubro = values[6];
        String monto = values[7];

        String[] t = new String[] {rubro, monto};

        output.collect(new Text(division), new TextArrayWritable(t));
    }
}
