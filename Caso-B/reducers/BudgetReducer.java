package reducers;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class BudgetReducer extends MapReduceBase implements Reducer<Text, FloatWritable,Text,FloatWritable> {

    public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text,FloatWritable> output, Reporter reporter) throws IOException {

        float sumPartido=0.0f;

        while (values.hasNext()) {
            sumPartido+=values.next().get();
        }

        output.collect(key,new FloatWritable(sumPartido));
    }
}
