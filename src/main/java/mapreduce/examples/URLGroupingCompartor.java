package mapreduce.examples;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by kpachipa on 6/17/2016.
 */
public class URLGroupingCompartor extends WritableComparator{
    public URLGroupingCompartor() {
        //super(URLCountPair.class, true);
    }
    @Override
    public int compare(WritableComparable url1, WritableComparable url2) {
        URLCountPair urlCountPair1 = (URLCountPair) url1;
        URLCountPair urlCountPair2 = (URLCountPair) url2;
        return urlCountPair1.getUrl().compareTo(urlCountPair2.getUrl());
    }
}
