package mapreduce.examples;

import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kpachipa on 6/17/2016.
 */
public class URLCountPair extends WritableComparator {

    // Some data

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }

    private String  url;
    private int counter;

    public void write(DataOutput out) throws IOException {
        out.writeChars(url);
        out.writeInt(counter);

    }

    public void readFields(DataInput in) throws IOException {
        url = in.readLine();
        counter = in.readInt();

    }

    public int compareTo(URLCountPair that) {

        int compareValue = this.url.compareTo(that.getUrl());
        if (compareValue == 0) {
            compareValue = Integer.valueOf(this.getCounter()).compareTo(Integer.valueOf(that.getCounter()));
        }
        return compareValue;
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + counter;
        result = prime * result + (int) (url.hashCode() ^ (url.hashCode() >>> 32));
        return result;
    }
}
