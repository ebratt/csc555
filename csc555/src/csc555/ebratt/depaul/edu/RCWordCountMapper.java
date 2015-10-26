package csc555.ebratt.depaul.edu;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

public class RCWordCountMapper extends 
	Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String author;
		String line = value.toString();
		String[] tuple = line.split("\\n");
		try {
			for (int i=0; i<tuple.length; i++) {
				JSONObject obj = new JSONObject(tuple[i]);
				author = obj.getString("author");
				context.write(new Text(author), one);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
}
