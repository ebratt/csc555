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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * GroupByCountPair is a class that allows for secondary sorting in a hadoop
 * framework. It works on Text and LongWritable objects under the assumption
 * that the user wants to group by and count to be something that cane be
 * sorted.
 * 
 * @author Eric Bratt
 * @version 11/11/2015
 * @since 11/11/2015
 * 
 */
public class GroupByCountPair implements Writable,
		WritableComparable<GroupByCountPair> {

	// instance variable to store the group by text
	private Text groupBy = new Text();
	// instance variable to store the count number
	private LongWritable count = new LongWritable();

	// default constructor
	public GroupByCountPair() {
	}

	// more typical constructor with signature of both instance fields
	public GroupByCountPair(String g, long c) {
		this.groupBy.set(g);
		this.count.set(c);
	}

	/**
	 * 
	 * Compares two GroupByCountPair objects and sorts them by count in a
	 * descending fashion. So, for example, "subreddit_1" and "subreddit_2"
	 * would be sorted as "subreddit_2", "subreddit_1".
	 * 
	 * @param in
	 *            the GroupByCountPair to compare to
	 * 
	 */
	@Override
	public int compareTo(GroupByCountPair in) {
		int compareValue = this.groupBy.compareTo(in.getGroupBy());
		if (compareValue == 0) {
			compareValue = count.compareTo(in.getCount());
		}
		return compareValue * (-1);
	}

	/**
	 * 
	 * Necessary for sorting.
	 * 
	 * @param o
	 *            the GroupByCountPair to compare to
	 * 
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		GroupByCountPair that = (GroupByCountPair) o;

		if (count != null ? !count.equals(that.count) : that.count != null)
			return false;
		if (groupBy != null ? !groupBy.equals(that.groupBy)
				: that.groupBy != null)
			return false;

		return true;
	}

	/**
	 * 
	 * Necessary for sorting to ensure no collisions.
	 * 
	 */
	@Override
	public int hashCode() {
		int result = groupBy != null ? groupBy.hashCode() : 0;
		result = 31 * result + (count != null ? count.hashCode() : 0);
		return result;
	}

	/**
	 * Required by Writable interface.
	 * 
	 * @param in
	 *            the DataInput
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		groupBy.readFields(in);
		count.readFields(in);
	}

	/**
	 * Required by Writable interface.
	 * 
	 * @param in
	 *            the DataInput
	 * @return the GroupByCountPair
	 * @throws IOException
	 *             in the event there is an issue with input/output
	 */
	public static GroupByCountPair read(DataInput in) throws IOException {
		GroupByCountPair groupByCountPair = new GroupByCountPair();
		groupByCountPair.readFields(in);
		return groupByCountPair;
	}

	/**
	 * Required by Writable interface.
	 * 
	 * @param out
	 *            the DataOutput
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		groupBy.write(out);
		count.write(out);
	}

	/**
	 * 
	 * @return this.groupBy the object's groupBy
	 */
	public Text getGroupBy() {
		return this.groupBy;
	}

	/**
	 * 
	 * @return this.count the object's count
	 */
	public LongWritable getCount() {
		return this.count;
	}

	/**
	 * 
	 * @param in
	 *            the value used to set this.count
	 */
	public void setCount(long in) {
		this.count.set(in);
	}

	/**
	 * 
	 * @param in
	 *            the value used to set this.groupBy
	 */
	public void setGroupBy(String in) {
		this.groupBy.set(in);
	}

	/**
	 * String representation of the pair. Example: "subreddit	1"
	 */
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.groupBy.toString());
		sb.append("\t");
		sb.append(this.count.toString());
		return sb.toString();
	}
}
