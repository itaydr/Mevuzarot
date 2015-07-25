package task1;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;



public class MText extends Text {

	private static final Text TMP = new Text();
	
	@Override
	public int compareTo(BinaryComparable other) {
		
		return super.compareTo(other);
	}

	@Override
	public int compareTo(byte[] other, int off, int len) {
		// TODO Auto-generated method stub
		return super.compareTo(other, off, len);
	}
	
	

}
