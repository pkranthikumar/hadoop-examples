package mapreduce.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class URLCounterTest {

	Mapper mapper = new URLCounterMapper();
	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = MapDriver.newMapDriver(mapper);
	Reducer reducer = new URLCounterReducer();
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver = ReduceDriver.newReduceDriver(reducer);
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	  
	  @Before
	  public void setUp() {
	  }

	@Test
	public void testMapOnly() throws IOException {
		mapDriver.getConfiguration().set("foo","bar");
		mapDriver.addAll(getInput());
		//mapDriver.withAllOutput(getOutput("output/url-mapper-output.txt"));
		//mapDriver.withCounter("DATA","line", getInputLines());
		mapDriver.runTest();

	}

	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.addAll(getInput());
		//mapReduceDriver.withAllOutput(getOutput("output/url-mapper-output.txt"));
		//mapReduceDriver.withCounter("DATA","line", getInputLines());
		//mapReduceDriver.withCounter("DATA","word", getOutputRecords());
		mapReduceDriver.runTest();
	}

	private static List<Pair<LongWritable, Text>> getInput(){
		Scanner scanner = new Scanner(Thread.currentThread().getContextClassLoader().getResourceAsStream("web-logs.csv"));
		List<Pair<LongWritable, Text>> input = new ArrayList<Pair<LongWritable, Text>>();
		while(scanner.hasNext()){
			String line = scanner.nextLine();
			input.add(new Pair<LongWritable, Text>(new LongWritable(0), new Text(line)));
		}
		return input;
	}

	private static int getInputLines(){
		Scanner scanner = new Scanner(URLCounterTest.class.getResourceAsStream("input/wordcount-input.txt"));
		int line = 0;
		while(scanner.hasNext()){
			scanner.nextLine();
			line++;
		}
		return line;
	}

	private static int getOutputRecords(){
		Scanner scanner = new Scanner(URLCounterTest.class.getResourceAsStream("output/wordcount-output.txt"));
		int record = 0;
		while(scanner.hasNext()){
			scanner.nextLine();
			record++;
		}
		return record;
	}

	private static List<Pair<Text,IntWritable>> getOutput(String fileName){
		Scanner scanner = new Scanner(URLCounterTest.class.getResourceAsStream("output/wordcount-output.txt"));
		List<Pair<Text,IntWritable>> output = new ArrayList<Pair<Text, IntWritable>>();
		while(scanner.hasNext()){
			String keyValue[] = scanner.nextLine().split(",");
			String word = keyValue[0];
			String count = keyValue[1];
			output.add(new Pair<Text, IntWritable>(new Text(word), new IntWritable(Integer.parseInt(count))));
		}
		return output;
	}

}
