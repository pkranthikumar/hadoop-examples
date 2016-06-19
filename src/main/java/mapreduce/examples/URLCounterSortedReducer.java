package mapreduce.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by kpachipa on 6/17/2016.
 */
public class URLCounterSortedReducer extends Reducer<LongWritable, Text, Text, LongWritable>{

    // Reuse objects.
    private final static IntWritable SUM = new IntWritable();

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Text url = null;
        Iterator<Text> iter = values.iterator();
        while (iter.hasNext()) {
            url =  iter.next();
        }

        context.write(url, key);
    }
}
