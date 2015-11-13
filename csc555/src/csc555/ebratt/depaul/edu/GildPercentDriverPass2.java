package csc555.ebratt.depaul.edu;

/*
 Copyright (c) 2015 Eric Bratt

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.

 The Software shall be used for Good, not Evil.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
 */

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import org.apache.log4j.Logger;

/**
 * GildPercentDriverPass2 is the hadoop class that drives the program. It parses
 * the reddit comments to determine what percentage of each group is "gilded".
 * Meaning, it counts the number of "gilded" comments divided by the total
 * number of comments within each group. Groups can be subreddits, authors, etc.
 * 
 * @author Eric Bratt
 * @version 11/11/2015
 * @since 11/11/2015
 * 
 */
public class GildPercentDriverPass2 extends Configured implements Tool {
	// static Logger log = Logger.getLogger("GuildPercentMapper");

	/**
	 * GuildPercentMapper is the hadoop class that maps the input.
	 * 
	 * @author Eric Bratt
	 * @version 11/11/2015
	 * @since 11/11/2015
	 * 
	 */
	public static class GildPercentMapperPass2 extends
			Mapper<LongWritable, Text, DoubleWritable, Text> {

		// instance variables for heap size reduction
		private DoubleWritable outKey = new DoubleWritable();
		private Text outValue = new Text();

		// Default constructor for inner-class
		public GildPercentMapperPass2() {
		};

		/**
		 * Parses the input values and emits the percentage followed by the key
		 * 
		 * <p>
		 * <b>Preconditions:</b>
		 * <ul>
		 * <li>the input values must be separated by new line character
		 * <li>the input value k,v must be delimited by tab character
		 * </ul>
		 * 
		 * <p>
		 * <b>Postconditions:</b>
		 * <ul>
		 * <li>emits (by example) "0.3333 reddit.com"
		 * </ul>
		 * 
		 * @param key
		 *            the byte offset as
		 *            {@link org.apache.hadoop.io.LongWritable}
		 * @param value
		 *            the input as {@link org.apache.hadoop.io.Text}
		 * @param context
		 *            the {@link org.apache.hadoop.mapreduce.Mapper.Context}
		 * @throws IOException
		 *             if there is an issue with input/output (like network
		 *             connection was lost during processing, ran out of space
		 *             trying to write, etc.)
		 * @throws InterruptedException
		 *             if something calls interrupt() on the thread.
		 * 
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException,
				ArrayIndexOutOfBoundsException {

			// a line in the input
			String[] line = value.toString().split("\\n");

			// loop over the lines
			for (String t : line) {
				// attempt to parse the percent gilded
				double gildPercent = Double.valueOf(t.split("\\t")[1]);
				String groupBy = t.split("\\t")[0];
				outKey.set(gildPercent);
				outValue.set(groupBy);
				context.write(outKey, outValue);
			}

		}
	}

	/**
	 * GildPercentReducerPass2 is the hadoop class that reduces the output of
	 * the GildPercentMapperPass2. It will emit the gild percent along with the
	 * group.
	 * 
	 * @author Eric Bratt
	 * @version 11/11/2015
	 * @since 11/11/2015
	 * 
	 */
	public static class GildPercentReducerPass2 extends
			Reducer<DoubleWritable, Text, DoubleWritable, Text> {

		// Default constructor for inner-class
		public GildPercentReducerPass2() {
		};

		/**
		 * 
		 * Just emits the top 10 since they're already sorted
		 * 
		 * @param key
		 *            the key from the mapper
		 *            {@link org.apache.hadoop.io.DoubleWritable}
		 * @param values
		 *            a list of {@link org.apache.hadoop.io.Text}
		 * @param context
		 *            the {@link org.apache.hadoop.mapreduce.Mapper.Context}
		 * @throws IOException
		 *             if there is an issue with input/output (like network
		 *             connection was lost during processing, ran out of space
		 *             trying to write, etc.)
		 * @throws InterruptedException
		 *             if something calls interrupt() on the thread.
		 * 
		 */
		public void reduce(DoubleWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Iterator<Text> itr = values.iterator();
			while (itr.hasNext()) {
				context.write(key, itr.next());
			}
		}
	}

	/**
	 * 
	 * Runs the driver by creating a new hadoop Job based on the configuration.
	 * Defines the path in/out based on the first two arguments. Allows for an
	 * optional combiner based on the 4th argument.
	 * 
	 * @param args
	 *            [0] the input directory on HDFS
	 * @param args
	 *            [1] the output directory on HDFS
	 * @param args
	 *            [2] tells the system whether or not to use a combiner ("yes")
	 *            and, if so, it will use the GildPercentReducerPass2.class as
	 *            the combiner.
	 * @throws Exception
	 *             if there is an issue with any of the arguments
	 * 
	 */
	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		StringBuffer sb = new StringBuffer();
		sb.append("sorted gild percent");
		job.setJobName(sb.toString());

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		// to ensure output is sorted
		job.setNumReduceTasks(1);

		// Mapper and Reducer Classes to use
		job.setMapperClass(GildPercentMapperPass2.class);
		job.setReducerClass(GildPercentReducerPass2.class);

		// Mapper output classes
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);

		// Input format class
		job.setInputFormatClass(TextInputFormat.class);

		// Reducer output classes
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		// Output format class
		job.setOutputFormatClass(TextOutputFormat.class);

		// Combiner
		if (args[2].equals("yes")) {
			job.setCombinerClass(GildPercentReducerPass2.class);
		}

		// sort in descending order
		job.setSortComparatorClass(DoubleWritableDescendingComparator.class);

		// The Jar file to run
		job.setJarByClass(GildPercentDriverPass2.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);

		return 0;
	};

	/**
	 * 
	 * This is the entry point to the program. It creates a new Configuration,
	 * checks the arguments, deletes the output directory on HDFS (if it already
	 * exists), sets the configuration properties 'aggregate' and 'groupBy',
	 * tells hadoop that it wants to compress the mapper output but not the
	 * reducer output, and runs the job.
	 * 
	 * @param args
	 *            [0] the input directory on HDFS
	 * @param args
	 *            [1] the output directory on HDFS
	 * @param args
	 *            [2] the combiner flag
	 * @throws Exception
	 *             if there is an issue with any of the arguments
	 * 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err
					.println("Usage: GildPercentPass2.jar <in> <out> <combiner? yes/no>");
			System.exit(2);
		}
		Path out = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(out)) {
			hdfs.delete(out, true);
		}

		// Enable mapper output compression, but not reducer
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.output.fileoutputformat.compress", "false");

		int res = ToolRunner.run(conf, new GildPercentDriverPass2(), args);
		System.exit(res);
	}
}