package task1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class DecadeNpmiPair implements Writable, WritableComparable {
	
	private int decade;
	private double npmi;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		decade = in.readInt();
	    npmi = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(decade);
		out.writeDouble(npmi);
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
    //@Override
    //public int compareTo(DecadeNpmiPair other) {
   // 	return 0;
    //}
/*	 private int year;
	    private String type;

	    public void write(DataOutput out) throws IOException {
	      out.writeInt(year);
	      out.writeBytes(type);
	    }

	    public void readFields(DataInput in) throws IOException {
	      year = in.readInt();
	      type = in.readBytes();
	    }

	    public static CrimeWritable read(DataInput in) throws IOException {
	      CrimeWritable w = new CrimeWritable();
	      w.readFields(in);
	      return w;
	  }
	  */
}
