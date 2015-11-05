package csc555.ebratt.depaul.edu;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupByGroupingComparator extends WritableComparator {
    public GroupByGroupingComparator() {
        super(GroupByCountPair.class, true);
    }
    
    @SuppressWarnings("rawtypes")
	@Override
    public int compare(WritableComparable gbcp1, WritableComparable gbcp2) {
        GroupByCountPair groupByCountPair = (GroupByCountPair) gbcp1;
        GroupByCountPair groupByCountPair2 = (GroupByCountPair) gbcp2;
        return groupByCountPair.getGroupBy().compareTo(groupByCountPair2.getGroupBy());
    }
}