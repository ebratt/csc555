package csc555.ebratt.depaul.edu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class RCTop10Driver extends Configured implements Tool {

	public static class RCTop10Mapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		// instance variables to reduce heap size
		private LongWritable count = new LongWritable();
		private Text text = new Text();
		
		// default constructor
		public RCTop10Mapper(){};

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			count.set(Long.parseLong(value.toString().split("\\t")[1]));
			text.set(value.toString().split("\\t")[0]);

			context.write(count, text);
		}
	}

	public static class RCTop10Reducer extends
			Reducer<LongWritable, Text, LongWritable, Text> {
		
		// instance variables to reduce heap size
		private Text text = new Text();
		
		// default constructor
		public RCTop10Reducer(){};
		
		// This caused a java heap error, so instead of concatenating
		// every word together of the same count, I'm just going to
		// emit the count and the word
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			Iterator<Text> itr = values.iterator();
			while (itr.hasNext()) {
				text = itr.next();
				context.write(key, text); 
			}
		}
	}

	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Top 10 Reddit");

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		// debugging
		// job.setNumReduceTasks(0);

		// Mapper and Reducer Classes to use
		job.setMapperClass(RCTop10Mapper.class);
		job.setReducerClass(RCTop10Reducer.class);

		// Mapper output classes
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		// input class
		job.setInputFormatClass(TextInputFormat.class);

		// Reducer output classes
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Force # of reduce tasks to 1
		job.setNumReduceTasks(1);

		// Tell Hadoop to sort in descending order
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		// The Jar file to run
		job.setJarByClass(RCTop10Driver.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);

		return 0;
	};

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: RCTop10 <in> <out>");
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
