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
import org.json.JSONException;
import org.json.JSONObject;

/**
 * GildPercentDriverPass1 is the hadoop class that drives the program. It parses the
 * reddit comments to determine what percentage of each group is "gilded".
 * Meaning, it counts the number of "gilded" comments divided by the total
 * number of comments within each group. Groups can be subreddits, authors, etc.
 * 
 * @author Eric Bratt
 * @version 11/11/2015
 * @since 11/11/2015
 * 
 */
public class GildPercentDriverPass1 extends Configured implements Tool {
//	static Logger log = Logger.getLogger("GuildPercentMapper");

	/**
	 * GuildPercentMapper is the hadoop class that maps the input.
	 * 
	 * @author Eric Bratt
	 * @version 11/11/2015
	 * @since 11/11/2015
	 * 
	 */
	public static class GildPercentMapperPass1 extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		// instance variables for heap size reduction
		private Text outKey = new Text();
		private DoubleWritable numGilded = new DoubleWritable();

		// Default constructor for inner-class
		public GildPercentMapperPass1() {
		};

		/**
		 * 
		 * Parses the input values into JSONObjects and emits a (key,value) pair
		 * as (groupBy,# gilded_# total). If the user selects '*' as the groupBy
		 * then it will emit the word 'ALL' as the groupBy in the key. Please
		 * refer to {@link org.json.JSONObject} and
		 * {@link org.json.JSONException} for more information about the JSON
		 * library used in this implementation. The "gilded" values are binary:
		 * 0 means that it was NOT gilded and 1 means that it was gilded.
		 * 
		 * <p>
		 * <b>Preconditions:</b>
		 * <ul>
		 * <li>there must be a configuration property named 'groupBy'
		 * <li>the input values must be separated by new line character
		 * <li>the input values must be JSON objects
		 * </ul>
		 * 
		 * <p>
		 * <b>Postconditions:</b>
		 * <ul>
		 * <li>emits (by example) "reddit.com 1.0" or "reddit.com 0.0"
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
		 * @throws JSONException
		 *             if the input data is malformed and cannot be parsed into
		 *             a JSONObject.
		 * @see org.json.JSONObject
		 * @see org.json.JSONException
		 * 
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// the text that we want to group by
			String groupBy = context.getConfiguration().get("groupBy");
			// a line in the input
			String[] tuple = value.toString().split("\\n");
			try {
				// loop over the lines
				for (String t : tuple) {
					// attempt to parse the line into a JSONObject
					JSONObject obj = new JSONObject(t);
					// get the gilded flag (0 or 1)
					numGilded.set((double)obj.getInt("gilded"));
					// if user wants to group by all
					if (groupBy.equals("*"))
						outKey.set("ALL");
					// otherwise group by the groupBy text
					else
						outKey.set(obj.getString(groupBy));
					context.write(outKey, numGilded);
				}
			} catch (JSONException e) { // capture JSONException
				e.printStackTrace();
			}
		}
	}

	/**
	 * GildPercentReducerPass1 is the hadoop class that reduces the output of the
	 * GildPercentMapperPass1. It will emit the group along with the gild percent as
	 * a DoubleWritable.
	 * 
	 * @author Eric Bratt
	 * @version 11/11/2015
	 * @since 11/11/2015
	 * 
	 */
	public static class GildPercentReducerPass1 extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		private DoubleWritable outValue = new DoubleWritable();

		// Default constructor for inner-class
		public GildPercentReducerPass1() {
		};

		/**
		 * 
		 * Aggregates the count of the gilded and total comments by group.
		 * 
		 * @param key
		 *            the key from the mapper {@link org.apache.hadoop.io.Text}
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
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double gildedSum = 0;
			double totalSum = 0;
			while (values.iterator().hasNext()) {
				gildedSum += values.iterator().next().get();
				totalSum += 1;
			}
			outValue.set(gildedSum / totalSum);
			context.write(key, outValue);
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
	 *            and, if so, it will use the GildPercentReducerPass1.class as the
	 *            combiner.
	 * @throws Exception
	 *             if there is an issue with any of the arguments
	 * 
	 */
	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		String groupBy = getConf().get("groupBy");
		StringBuffer sb = new StringBuffer();
		sb.append("gild percent of: ");
		sb.append(groupBy);
		job.setJobName(sb.toString());

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		// debugging
//		job.setNumReduceTasks(0);

		// Mapper and Reducer Classes to use
		job.setMapperClass(GildPercentMapperPass1.class);
		job.setReducerClass(GildPercentReducerPass1.class);

		// Mapper output classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		// Input format class
		job.setInputFormatClass(TextInputFormat.class);

		// Reducer output classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Output format class
		job.setOutputFormatClass(TextOutputFormat.class);

		// Combiner
		if (args[2].equals("yes")) {
			job.setCombinerClass(GildPercentReducerPass1.class);
		}

		// The Jar file to run
		job.setJarByClass(GildPercentDriverPass1.class);

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
	 *            [2] tells the system whether or not to use a combiner ("yes")
	 *            and, if so, it will use the GildPercentReducerPass1.class as the
	 *            combiner.
	 * @param args
	 *            [3] the JSON key to group by
	 * @throws Exception
	 *             if there is an issue with any of the arguments
	 * 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 4) {
			System.err
					.println("Usage: GildPercentPass1.jar <in> <out> <combiner? yes/no> <group by '*' for all>");
			System.exit(2);
		}
		Path out = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(out)) {
			hdfs.delete(out, true);
		}
		conf.set("groupBy", args[3]);

		// Enable mapper output compression, but not reducer
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.output.fileoutputformat.compress", "false");

		int res = ToolRunner.run(conf, new GildPercentDriverPass1(), args);
		System.exit(res);
	}
}