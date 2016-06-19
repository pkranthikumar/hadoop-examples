package mapreduce.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by kpachipa on 6/17/2016.
 */
public class URLCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    // Reuse objects to save overhead of object creation.
    private final static IntWritable ONE = new IntWritable(1);
    private final static Text URL = new Text();

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String line = ((Text) value).toString();
        String [] fields = line.split(",");
        if (fields.length < 6) return;
        String url = fields[5];
        URL.set(url);
        context.write(new Text(url), ONE);
    }
}
