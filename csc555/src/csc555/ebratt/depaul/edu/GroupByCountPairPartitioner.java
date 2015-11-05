package csc555.ebratt.depaul.edu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupByCountPairPartitioner extends Partitioner<GroupByCountPair, Text> {
	
	public GroupByCountPairPartitioner() { }

	@Override
	public int getPartition(GroupByCountPair groupByCountPair, Text text, int numPartitions) {
		return Math.abs(groupByCountPair.getGroupBy().hashCode()) % numPartitions;
	}

}
