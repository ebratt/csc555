package csc555.ebratt.depaul.edu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RCWordCountDriver extends Configured implements Tool {
	
	static Configuration _conf = new Configuration();

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(_conf, "Reddit Word Count of: author");
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		// debugging
//		job.setNumReduceTasks(0);

		// Mapper and Reducer Classes to use
		job.setMapperClass(RCWordCountMapper.class);
		job.setReducerClass(RCWordCountReducer.class);
		
		// Mapper output classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// Reducer input class
		job.setInputFormatClass(TextInputFormat.class);
		
		// Reducer output classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// The Jar file to run
		job.setJarByClass(RCWordCountDriver.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);

		return 0;
	};

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: RCWordCountDriver <in> <out>");
			System.exit(2);
		}
		Path out = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(_conf);
		if (hdfs.exists(out)) { hdfs.delete(out, true); }
		int res = ToolRunner.run(_conf, new RCWordCountDriver(), args);
		System.exit(res);
	}

}
