package csc555.ebratt.depaul.edu;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingVIntWritableComparable extends WritableComparator {

	public DescendingVIntWritableComparable() { super(VIntWritable.class); }

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

		Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
		Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

		return v1.compareTo(v2) * (-1);
	}
}