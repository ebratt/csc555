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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * RCTop10Driver is the hadoop class that drives the program. It is intended as
 * a second step in a mult-step MapReduce application. What this program does is
 * counts the number of occurrences of a word by group and emits the top 10
 * most-frequently-occurring words in the group.
 * 
 * @author Eric Bratt
 * @version 11/11/2015
 * @since 11/11/2015
 * 
 */
public class RCTop10Driver extends Configured implements Tool {

	public static Logger log = Logger.getLogger(RCTop10Driver.class);

	/**
	 * RCTop10Mapper is the hadoop class that maps the input.
	 * 
	 * @author Eric Bratt
	 * @version 11/11/2015
	 * @since 11/11/2015
	 * 
	 */
	public static class RCTop10Mapper extends Mapper<Text, Text, GroupByCountPair, Text> {

		// instance variable for heap size reduction and sorting
		private GroupByCountPair groupByCountPair = new GroupByCountPair();
		private Text aggregate = new Text();
		private String groupBy = new String();
		private String countString = new String();

		// default constructor for inner-class
		public RCTop10Mapper() {
		};

		/**
		 * Parses the input values and emits a (key,value) pair as
		 * (GroupByCountPair, Text). Examples of input are:
		 * <ul>
		 * <li>reddit.com_is 1
		 * <li>reddit.com_is 1
		 * <li>reddit.com_had 1
		 * <li>reddit.com_had 1
		 * </ul>
		 * 
		 * Examples of output are:
		 * <ul>
		 * <li>reddit.com 2 is
		 * <li>reddit.com 2 had
		 * </ul>
		 * 
		 * @param key
		 *            the groupBy_aggregate as {@link org.apache.hadoop.io.Text}
		 * @param value
		 *            the count as {@link org.apache.hadoop.io.Text}
		 * @param context
		 *            the {@link org.apache.hadoop.mapreduce.Mapper.Context}
		 * @throws IOException
		 *             if there is an issue with input/output (like network
		 *             connection was lost during processing, ran out of space
		 *             trying to write, etc.)
		 * @throws InterruptedException
		 *             if something calls interrupt() on the thread.
		 */
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			try {
				// parse the input
				// get the value to count
				aggregate.set(key.toString().split("_")[1]);
				// get the value to group by
				groupBy = key.toString().split("_")[0];
				// get the value to sum
				countString = value.toString();
				// skip blanks
				if (!(groupBy.equals(null)) && !(aggregate.equals(null)) && !(countString.equals(null))) {
					long count = Long.parseLong(countString);
					groupByCountPair.setGroupBy(groupBy);
					groupByCountPair.setCount(count);
					context.write(groupByCountPair, aggregate);
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				log.info("key: " + key.toString() + "; value: " + value.toString());
			}
		}
	}

	/**
	 * RCTop10Reducer is the hadoop class that reduces the output of the
	 * RCTop10Mapper.
	 * 
	 * @author Eric Bratt
	 * @version 11/11/2015
	 * @since 11/11/2015
	 * 
	 */
	public static class RCTop10Reducer extends Reducer<GroupByCountPair, Text, GroupByCountPair, Text> {

		// instance variables to reduce heap size
		private Text text = new Text();

		// default constructor for inner-class
		public RCTop10Reducer() {
		};

		// This caused a java heap error, so instead of concatenating
		// every word together of the same count, I'm just going to
		// emit the count and the word
		/**
		 * @param key
		 *            example: reddit.com 1
		 * @param values
		 *            example: addicted the
		 * @param context
		 *            the {@link org.apache.hadoop.mapreduce.Mapper.Context}
		 * @throws IOException
		 *             if there is an issue with input/output (like network
		 *             connection was lost during processing, ran out of space
		 *             trying to write, etc.)
		 * @throws InterruptedException
		 *             if something calls interrupt() on the thread.
		 */
		public void reduce(GroupByCountPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// local variable to increment the key count so that we only
			// emit the top 10 entries
			int count = 0;
			Iterator<Text> itr = values.iterator();
			while (itr.hasNext() && count < 10) {
				count++;
				text = itr.next();
				context.write(key, text);
			}
		}
	}

	/**
	 * 
	 * Runs the driver by creating a new hadoop Job based on the configuration.
	 * Defines the path in/out based on the first two arguments.
	 * 
	 * @param args
	 *            [0] the input directory on HDFS
	 * @param args
	 *            [1] the output directory on HDFS
	 * @throws Exception
	 *             if there is an issue with any of the arguments
	 * 
	 */
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Top 10 Reddit");

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		// ensure 1 reduce tasks for ranking
		job.setNumReduceTasks(1);

		// Mapper and Reducer Classes to use
		job.setMapperClass(RCTop10Mapper.class);
		job.setReducerClass(RCTop10Reducer.class);

		// Mapper output classes
		job.setMapOutputKeyClass(GroupByCountPair.class);
		job.setMapOutputValueClass(Text.class);

		// set custom partitioner
		job.setPartitionerClass(GroupByCountPairPartitioner.class);

		// set custom grouping comparator
		job.setGroupingComparatorClass(GroupByGroupingComparator.class);

		// input class
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// Reducer output classes
		job.setOutputKeyClass(GroupByCountPair.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// The Jar file to run
		job.setJarByClass(RCTop10Driver.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);

		return 0;
	};

	/**
	 * 
	 * This is the entry point to the program. It creates a new Configuration,
	 * checks the arguments, deletes the output directory on HDFS (if it already
	 * exists), tells hadoop that it wants to compress the mapper output but not
	 * the reducer output, and runs the job.
	 * 
	 * @param args
	 *            [0] the input directory on HDFS
	 * @param args
	 *            [1] the output directory on HDFS
	 * @throws Exception
	 *             if there is an issue with any of the arguments
	 * 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: RCTop10.jar <in> <out>");
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

		int res = ToolRunner.run(conf, new RCTop10Driver(), args);
		System.exit(res);
	}

}
