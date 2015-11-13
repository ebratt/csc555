package csc555.ebratt.depaul.edu;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DoubleWritableDescendingComparator extends WritableComparator {

	// Constructor.
	protected DoubleWritableDescendingComparator() {
		super(DoubleWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		DoubleWritable k1 = (DoubleWritable) w1;
		DoubleWritable k2 = (DoubleWritable) w2;

		return -1 * k1.compareTo(k2);
	}
}
