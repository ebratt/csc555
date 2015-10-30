package csc555.ebratt.depaul.edu;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.json.JSONException;
import org.json.JSONObject;

public class RCWordCountDriver extends Configured implements Tool {

	public static class RCWordCountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		// instance variables for heap size
		private Text wordText = new Text();

		// Default constructor
		public RCWordCountMapper() {
		};

		private static final IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String word = context.getConfiguration().get("word");
			String[] tuple = value.toString().split("\\n");
			try {
				for (int i = 0; i < tuple.length; i++) {
					JSONObject obj = new JSONObject(tuple[i]);
					String jsonString = obj.getString(word);
					StringTokenizer itr = new StringTokenizer(jsonString);
					while (itr.hasMoreTokens()) {
						String tmp = itr.nextToken()
								.replaceAll("[^\\dA-Za-z ]", "")
								.replaceAll("\\s+", "+")
								.toLowerCase();
						wordText.set(tmp);
						context.write(wordText, one);
					}
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}

	public static class RCWordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public RCWordCountReducer() {
		};

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			while (values.iterator().hasNext()) {
				sum += values.iterator().next().get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		String word = getConf().get("word");
		StringBuffer sb = new StringBuffer();
		sb.append("Reddit Word Count of: ");
		sb.append(word);
		job.setJobName(sb.toString());

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		// debugging
		// job.setNumReduceTasks(0);

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
		
		// Combiner
		if (args[3].equals("yes")) {
			job.setCombinerClass(RCWordCountReducer.class);
		}

		// The Jar file to run
		job.setJarByClass(RCWordCountDriver.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);

		return 0;
	};

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 4) {
			System.err.println(
			"Usage: RCWordCountDriver <in> <out> <word> <combiner? yes/no>");
			System.exit(2);
		}
		Path out = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(out)) {
			hdfs.delete(out, true);
		}
		conf.set("word", args[2]);
		
		// Enable mapper output compression, but not reducer
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.output.fileoutputformat.compress", "false");
		
		int res = ToolRunner.run(conf, new RCWordCountDriver(), args);
		System.exit(res);
	}

}
