package mapreduce.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by kpachipa on 6/17/2016.
 */
public class URLCounterSortedMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

    // Reuse objects to save overhead of object creation.
    private final static IntWritable ONE = new IntWritable(1);
    private final static Text URL = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
         String[] fields = value.toString().split("\t");
        context.write(new LongWritable(Long.parseLong(fields[1])), new Text(fields[0]));
    }
}
