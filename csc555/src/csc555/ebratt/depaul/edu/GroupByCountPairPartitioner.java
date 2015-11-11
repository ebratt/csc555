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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * GroupByCountPairPartitioner is a custom partitioner used in a hadoop job to
 * ensure that GroupByCountPair objects are sent to the same reducer based on
 * their groupBy values. This is so that each reducer works on the same set of
 * groupBy key values and can perform the sorting at that level.
 * 
 * @author Eric Bratt
 * @version 11/11/2015
 * @since 11/11/2015
 * 
 */
public class GroupByCountPairPartitioner extends
		Partitioner<GroupByCountPair, Text> {

	// default constructor
	public GroupByCountPairPartitioner() {
	}

	/**
	 * @param groupByCountPair
	 *            The GroupByCountPair for which to calculate the partition
	 *            number. This is the key from the mapper output.
	 * @param text
	 *            Required by the Partitioner interface. This is the value from
	 *            the mapper output.
	 * @param numPartitions
	 *            The number of reduce tasks for the job
	 * @return An integer representing the partition
	 */
	@Override
	public int getPartition(GroupByCountPair groupByCountPair, Text text,
			int numPartitions) {
		return Math.abs(groupByCountPair.getGroupBy().hashCode())
				% numPartitions;
	}

}
