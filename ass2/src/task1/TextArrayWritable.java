package task1;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextArrayWritable extends ArrayWritable implements WritableComparable {
	
		public TextArrayWritable() {
			super(Text.class);
		}
		
	 	public TextArrayWritable(Text[] values) {
	        super(Text.class, values);
	    }

	    @Override
	    public Text[] get() {
	        return (Text[]) super.get();
	    }

	    @Override
	    public String toString() {
	    	Text[] values = get();
	    	String str = "";
	    	for (int i = 0 ; i < values.length ; i++) {
	    		str += values[i].toString() + " ";
	    	}
	        return str;
	    }

		@Override
		public int compareTo(Object o) {
			if (o.getClass().isInstance(this.getClass()) && this.getValueClass().equals(((ArrayWritable)o).getValueClass())) {
				Text me[] = this.get();
				Text other[] = ((TextArrayWritable)o).get();
				if (me.length != other.length) {
					return me.length > other.length ? 1 : -1;
				}
				else {
					for (int i = 0 ; i < me.length ; i++) {
						int eq = me[i].compareTo(other[i]);
						if (eq != 0) {
							return eq;
						}
					}
				}
			}
			else {
				return -1;
			}
			
			return 0;
		}
}
