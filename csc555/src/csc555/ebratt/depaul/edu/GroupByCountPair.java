package csc555.ebratt.depaul.edu;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class GroupByCountPair implements Writable, WritableComparable<GroupByCountPair> {

	private Text groupBy = new Text();
	private LongWritable count = new LongWritable();

	public GroupByCountPair() { }

	public GroupByCountPair(String g, long c) {
		this.groupBy.set(g);
		this.count.set(c);
	}

	@Override
	public int compareTo(GroupByCountPair in) {
		int compareValue = this.groupBy.compareTo(in.getGroupBy());
		if (compareValue == 0) {
			compareValue = count.compareTo(in.getCount());
		}
		return compareValue * (-1);
	}
	
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GroupByCountPair that = (GroupByCountPair) o;

        if (count != null ? !count.equals(that.count) : that.count != null) return false;
        if (groupBy != null ? !groupBy.equals(that.groupBy) : that.groupBy != null) return false;

        return true;
    }
    
    @Override
    public int hashCode() {
        int result = groupBy != null ? groupBy.hashCode() : 0;
        result = 31 * result + (count != null ? count.hashCode() : 0);
        return result;
    }

	@Override
	public void readFields(DataInput in) throws IOException {
		groupBy.readFields(in);
		count.readFields(in);
	}

	public static GroupByCountPair read(DataInput in) throws IOException {
		GroupByCountPair groupByCountPair = new GroupByCountPair();
		groupByCountPair.readFields(in);
		return groupByCountPair;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		groupBy.write(out);
		count.write(out);
	}
	
	public Text getGroupBy() { return this.groupBy; }
	public LongWritable getCount() { return this.count; }
	public void setCount(long in) { this.count.set(in); }
	public void setGroupBy(String in) { this.groupBy.set(in); }

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.groupBy.toString());
		sb.append("\t");
		sb.append(this.count.toString());
		return sb.toString();
	}
}
