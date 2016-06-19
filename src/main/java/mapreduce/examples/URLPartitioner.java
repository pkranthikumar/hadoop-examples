package mapreduce.examples;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by kpachipa on 6/17/2016.
 */
public class URLPartitioner extends Partitioner<URLCountPair, NullWritable> {

    @Override
    public int getPartition(URLCountPair urlCountPair, NullWritable nullWritable, int numPartitions) {
        return urlCountPair.getUrl().hashCode() % numPartitions;
    }
}
