package mapreduce;

import java.io.IOException;

import model.NGram;
import model.NGramFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import Utils.Constants;
import Utils.DLogger;

public class MICalculator {
	
	final static DLogger L = new DLogger(true);
	public final static int P_SLOTX_W1_LENGTH = 5;
	public final static int WILD_SLOTX_WILD_LENGTH = 5;
	public final static int P_SLOTX_WILD_LENGTH = 8;
	public final static int WILD_SLOTX_W1_LENGTH = 8;
	public final static int P_SLOTY_W2_LENGTH = 8;
	public final static int WILD_SLOTY_WILD_LENGTH = 8;
	public final static int P_SLOTY_WILD_LENGTH = 8;
	public final static int WILD_SLOTY_W2_LENGTH = 8;
	
	/**************************
	 * 
	 * Possible Input 
	 * 1. <type, p, slotX, w1> -> <count, total>	[5]
	 * 2. <type, p, slotY, w2> -> <Count, total>	[5]
	 * 3. <type, *, slotX, *> -> <Ngram, total>		[8]
	 * 4. <type, *, slotY, *> -> <NGram, total>		[8]
	 * 5. <type, p, slotX, *> -> <Ngram, total>		[8]
	 * 6. <type, p, slotY, *> -> <NGram, total>		[8]
	 * 7. <type, *, slotX, w1> -> <Ngram, total>	[8]
	 * 8. <type, *, slotY, w2> -> <NGram, total>	[8]
	 * 
	 * This mapper accepts as input the out of MIInfoExtractorReducer's output.
	 * Output - 
	 * 	1. <p, slotY, w2> -> 
	 * 
	 * 
	 * Input is the initial ngram input files.
	 */
	public static class MICalculatorMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		// Objects for reuse
		private final static Text Key = new Text();
		private final static Text Val = new Text();
		
		private void emit(Context context, String p, String slot, String w, String identifier, String count) 
				throws IOException, InterruptedException {
			Key.set(p + Constants.S + slot + Constants.S + w);
			Val.set(identifier + Constants.S + count);
			context.write(Key, Val);
		}
				
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] arr = value.toString().split(Constants.S);
			
			if (arr.length == 0) {
				L.log("Something bad happened - " + value);
				return;
			}
						
			String type = arr[0];
			if (type.equals(Constants.P_SLOTX_W1)) {
				if (arr.length != 9) {L.log("Bad length" + value); return;}
				String p = arr[1];
				String w = arr[3];
				String count = arr[5];
				emit(context, p, Constants.SLOT_X, w, Constants.P_SLOTX_W1, count);
			}
			else if (type.equals(Constants.WILD_SLOTX_WILD)) {
				if (arr.length != WILD_SLOTX_WILD_LENGTH) {L.log("Bad length (" + arr.length + ")" + value); return;}
				String p = arr[1];
				String w = arr[3];
				String count = arr[5];
				emit(context, p, Constants.SLOT_X, w, Constants.WILD_SLOTX_WILD, count);
			}
			else if (type.equals(Constants.P_SLOTX_WILD)) {
				if (arr.length != P_SLOTX_WILD_LENGTH) {L.log("Bad length (" + arr.length + ")" + value); return;}
				String p = arr[1];
				String w = arr[4];
				String count = arr[8];
				emit(context, p, Constants.SLOT_X, w, Constants.P_SLOTX_WILD, count);
			}
			else if (type.equals(Constants.WILD_SLOTX_W1)) {
				if (arr.length != WILD_SLOTX_W1_LENGTH) {L.log("Bad length (" + arr.length + ")" + value); return;}
				String p = arr[6];
				String w = arr[4];
				String count = arr[8];
				emit(context, p, Constants.SLOT_X, w, Constants.WILD_SLOTX_W1, count);
			}
			else if (type.equals(Constants.P_SLOTY_W2)) {
				if (arr.length != P_SLOTY_W2_LENGTH) {L.log("Bad length (" + arr.length + ")" + value); return;}
				String p = arr[1];
				String w = arr[3];
				String count = arr[8];
				emit(context, p, Constants.SLOT_Y, w, Constants.P_SLOTY_W2, count);
			}
			else if (type.equals(Constants.WILD_SLOTY_WILD)) {
				if (arr.length != WILD_SLOTY_WILD_LENGTH) {L.log("Bad length  (" + arr.length + ")" + arr.length + ")" + value); return;}
				String p = arr[6];
				String w = arr[5];
				String count = arr[8];
				emit(context, p, Constants.SLOT_Y, w, Constants.WILD_SLOTY_WILD, count);
			
			}
			else if (type.equals(Constants.P_SLOTY_WILD)) {
				if (arr.length != P_SLOTY_WILD_LENGTH) {L.log("Bad length (" + arr.length + ")" + value); return;}
				String p = arr[1];
				String w = arr[5];
				String count = arr[8];
				emit(context, p, Constants.SLOT_Y, w, Constants.P_SLOTY_WILD, count);
			
			}
			else if (type.equals(Constants.WILD_SLOTY_W2)) {
				if (arr.length != WILD_SLOTY_W2_LENGTH) {L.log("Bad length (" + arr.length + ")" + value); return;}
				String p = arr[6];
				String w = arr[3];
				String count = arr[8];
				emit(context, p, Constants.SLOT_Y, w, Constants.WILD_SLOTY_W2, count);
			}
			else {
				L.log("Unrecognized type = " + key);
				return;
			}
		}
	}
	
	/**
	 * 
	 * Input - @see MICalculatorMapper. Can accept any of it's outputs.
	 * 
	 * Output - 
	 * 		1. <p, slotX, w1, count, mi>
	 *  	2. <p, slotY, w2, count, mi>
	 * 	
	 * @author asaf
	 *
	 */
	public static class MICalculatorReducer extends
			Reducer<Text, Text, Text, Text> {
		// Reuse objects

		private final static Text KEY = new Text();
		private final static Text VAL = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			
		}
	}
}
