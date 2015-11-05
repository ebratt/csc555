package csc555.ebratt.depaul.edu;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupByCountPairPartitioner extends Partitioner<GroupByCountPair, Text> implements Configurable {
	
	Configuration conf = new Configuration();
	
	public GroupByCountPairPartitioner() { }

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int getPartition(GroupByCountPair groupByCountPair, Text text, int numPartitions) {
		return groupByCountPair.getGroupBy().hashCode() % numPartitions;
	}

}
