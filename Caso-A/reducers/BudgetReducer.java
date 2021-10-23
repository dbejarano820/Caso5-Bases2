package reducers;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import mapers.TextArrayWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class BudgetReducer extends MapReduceBase implements Reducer<Text, TextArrayWritable,Text,Text> {

    private TreeMap<Long, String> tmap;
    
    public void reduce(Text key, Iterator<TextArrayWritable> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {

        tmap = new TreeMap<Long, String>();

        while (values.hasNext()) {

            String[] w = values.next().toStrings();

            String rubro = w[0];
            String monto;

            if(w[1].equals("")){
                monto = "0";
            } else{
                monto = w[1];
            }

            if( tmap.containsValue(rubro)) {
                for(Map.Entry<Long, String> entry: tmap.entrySet()) {
                    String s = entry.getValue();
                    if(s.equals(rubro)){
                        long l = entry.getKey();
                        tmap.remove(entry.getKey());
                        tmap.put(l+Long.valueOf(monto), rubro);
                        break;
                    }
                }
            } else {
                tmap.put(Long.valueOf(monto), rubro);
            }

        }

        while( tmap.size() > 3) {
            tmap.remove(tmap.firstKey());
        }

        String res = "\n";
        int i = 3;

        for(Map.Entry<Long, String> entry: tmap.entrySet()) {
            Long k = entry.getKey();
            String s = entry.getValue();
            res += "  " + i + "." + s + "  Presupuesto: " +  k + "\n";
            i -= 1;
        }

        output.collect(key,new Text(res));
    }
}
