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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * GroupByGroupingComparator is a custom comparator used in a hadoop job to
 * ensure that GroupByCountPair objects are grouped using their groupBy fields.
 * 
 * @author Eric Bratt
 * @version 11/11/2015
 * @since 11/11/2015
 * 
 */
public class GroupByGroupingComparator extends WritableComparator {

	// default constructor
	public GroupByGroupingComparator() {
		super(GroupByCountPair.class, true);
	}

	/**
	 * @param gbcp1
	 *            one of two GroupByCountPair objects to compare
	 * @param gbcp2
	 *            one of two GroupByCountPair objects to compare
	 * @return An integer representing the comparison of the two objects based
	 *         on their groupBy fields.
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable gbcp1, WritableComparable gbcp2) {
		GroupByCountPair groupByCountPair = (GroupByCountPair) gbcp1;
		GroupByCountPair groupByCountPair2 = (GroupByCountPair) gbcp2;
		return groupByCountPair.getGroupBy().compareTo(
				groupByCountPair2.getGroupBy());
	}
}