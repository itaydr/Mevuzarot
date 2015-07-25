package task1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FinalOutputWritable implements WritableComparable {

	public int decade;
    public double npmi;
    public boolean isSecondary;
    
    public void write(DataOutput out) throws IOException {
      out.writeInt(decade);
      out.writeDouble(npmi);
      out.writeBoolean(isSecondary);
    }
    
    public void readFields(DataInput in) throws IOException {
    	decade = in.readInt();
    	npmi = in.readDouble();
    	isSecondary = in.readBoolean();
    }
    
    public int compareToFinalOutputWritable(FinalOutputWritable o) {
      if (this.decade == o.decade) {
    	  if ( this.isSecondary == o.isSecondary) {
    		  if ( this.npmi == o.npmi) {
    			  return 0;
    		  }
    		  else {
    			  return this.npmi > o.npmi ? -1 : 1; 
    		  }
    	  }
    	  else {
    		  return this.isSecondary ? -1 : 1;
    	  }
      }
      else {
    	  return this.decade > o.decade ? -1 : 1;
      }
    }

	@Override
	public int compareTo(Object o) {
		//System.out.println("comp - " + o + " " + this);
		
		if (o != null && o instanceof FinalOutputWritable) {
			return this.compareToFinalOutputWritable((FinalOutputWritable) o);
		}
		
		System.out.println("Not same class - " + o.getClass() + " " + this.getClass());
		
		return 0;
	}
	
	
	@Override
	public int hashCode() {
		System.out.println("hashCode " + this);
		
		return super.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		System.out.println("equals same class - " + this);
		
		return super.equals(obj);
	}

	@Override
	public String toString() {
		return String.valueOf(decade) + " " + String.valueOf(npmi);
	}
	
	
}
