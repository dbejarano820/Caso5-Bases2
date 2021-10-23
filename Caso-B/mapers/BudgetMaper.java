package mapers;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class BudgetMaper extends MapReduceBase implements Mapper<LongWritable,Text,Text,FloatWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text,FloatWritable> output, Reporter reporter) throws IOException{
        String line = value.toString();
        String values[] = line.split(",");

        String partido = values[4];
        String monto = values[7];

        if(monto.equals("")){
            monto = "0";
        }

        output.collect(new Text(partido), new FloatWritable(Float.valueOf(monto)));
    }
}
