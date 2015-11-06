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

	public static class RCTop10Mapper extends Mapper<LongWritable, Text, GroupByCountPair, Text> {
		
		private GroupByCountPair groupByCountPair = new GroupByCountPair();

		// default constructor
		public RCTop10Mapper(){  };

		/**
		 * @param value: example: "reddit.com_addicted	1"
		 * @return void: but emits "reddit.com	1	addicted"
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// parse the input
			String line = value.toString();
			try {
				String aggregate = line.split("\\t")[0].split("_")[1];
				String groupBy = line.split("\\t")[0].split("_")[0];
				String countString = line.split("\\t")[1];
				if (!(groupBy.equals(null)) && 
						!(aggregate.equals(null)) && 
						!(countString.equals(null))) {
					long count = Long.parseLong(countString);
					groupByCountPair.setGroupBy(groupBy);
					groupByCountPair.setCount(count);
					context.write(groupByCountPair, new Text(aggregate));
				}
				
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
				// what do we do if we get this?
			}
		}
	}

	public static class RCTop10Reducer extends
			Reducer<GroupByCountPair, Text, GroupByCountPair, Text> {
		
		// instance variables to reduce heap size
		private Text text = new Text();
		
		// default constructor
		public RCTop10Reducer(){};
		
		// This caused a java heap error, so instead of concatenating
		// every word together of the same count, I'm just going to
		// emit the count and the word
		/**
		 * @param key: example: reddit.com	1
		 * @param values: example: [addicted,the,...]
		 */
		@Override
		protected void reduce(GroupByCountPair key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			int count = 0;
			Iterator<Text> itr = values.iterator();
			while (itr.hasNext() && count < 10) {
				count++;
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
//		 job.setNumReduceTasks(0);

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
		job.setInputFormatClass(TextInputFormat.class);

		// Reducer output classes
		job.setOutputKeyClass(GroupByCountPair.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Force # of reduce tasks to 1
//		job.setNumReduceTasks(1);

		// The Jar file to run
		job.setJarByClass(RCTop10Driver.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);

		return 0;
	};

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
