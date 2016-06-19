package mapreduce.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.util.Random;

/**
 * Simple word count demo.
 */
public class URLCounterSorted extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(URLCounterSorted.class);
  /**
   * Creates an instance of this tool.
   */
  public URLCounterSorted() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + URLCounterSorted.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(URLCounterSorted.class.getSimpleName());
    job.setJarByClass(URLCounterSorted.class);

    job.setNumReduceTasks(args.numReducers);

    Path tempDir =
            new Path("count-temp-"+
                    Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, tempDir);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(URLCounterMapper.class);
    job.setCombinerClass(URLCounterReducer.class);
    job.setReducerClass(URLCounterReducer.class);



    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Finished Job 1 in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    LOG.info("Starting Job2 " );
    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);
    Job sortJob = new Job(conf);
    sortJob.setJobName("count-sort");
    sortJob.setJarByClass(URLCounterSorted.class);
    FileInputFormat.setInputPaths(sortJob, tempDir);
    sortJob.setMapperClass(URLCounterSortedMapper.class);
    sortJob.setReducerClass(URLCounterSortedReducer.class);
    sortJob.setNumReduceTasks(1);                 // write a single file
    FileOutputFormat.setOutputPath(sortJob, outputDir);
    sortJob.setSortComparatorClass(          // sort by decreasing freq
            LongWritable.DecreasingComparator.class);
    sortJob.waitForCompletion(true);

    FileSystem.get(conf).delete(tempDir, true);

    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new URLCounterSorted(), args);
  }
}
