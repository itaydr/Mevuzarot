package mapreduce;

import huristics.MeniHueristics;
import huristics.PaperHuristics;

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
	
	final static DLogger L = new DLogger(true, "MICalculator");
	public final static int P_SLOTX_W1_LENGTH = 6;
	public final static int WILD_SLOTX_WILD_LENGTH = 9;
	public final static int P_SLOTX_WILD_LENGTH = 9;
	public final static int WILD_SLOTX_W1_LENGTH = 9;
	public final static int P_SLOTY_W2_LENGTH = 6;
	public final static int WILD_SLOTY_WILD_LENGTH = 9;
	public final static int P_SLOTY_WILD_LENGTH = 9;
	public final static int WILD_SLOTY_W2_LENGTH = 9;
	
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
	 * 	1. <p, slotY, w2> -> <type, count> 
	 *  2. <p, slotX, w1> -> <type, count>
	 * 
	 * Input is the initial ngram input files.
	 */
	public static class MICalculatorMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		// Objects for reuse
		private final static Text Key = new Text();
		private final static Text Val = new Text();
		private static double DPMinCount = 0.0;
		
		private void emit(Context context, String p, String slot, String w, String identifier, String total, String ngramCount) 
				throws IOException, InterruptedException {
			Key.set(p + Constants.S + slot + Constants.S + w);
			Val.set(identifier + Constants.S + total + Constants.S + ngramCount);
			context.write(Key, Val);
		}
		
		@Override
		public void setup(Context context) throws IOException {
			DPMinCount = context.getConfiguration().getDouble("DPMinCount", 0.0);
		}
		
				
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] arr = value.toString().trim().split(Constants.S);
			
			if (arr.length == 0) {
				L.log("Something bad happened - " + value);
				return;
			}
						
			String type = arr[0];
			if (type.equals(Constants.P_SLOTX_W1)) {
				if (arr.length != P_SLOTX_W1_LENGTH) {L.log("Bad length (" + arr.length + ")" + value);return;}
				String p = arr[1];
				String w = arr[3];
				String count = arr[5];
				String ngramCount = arr[4];
				emit(context, p, Constants.SLOT_X, w, Constants.P_SLOTX_W1, count ,ngramCount);
			}
			else if (type.equals(Constants.WILD_SLOTX_WILD)) {
				if (arr.length != WILD_SLOTX_WILD_LENGTH) {L.log("Bad length (" + arr.length + ")" + value); return;}
				String p = arr[6];
				String w = arr[4];
				String count = arr[8];
				String ngramCount = arr[7];
				emit(context, p, Constants.SLOT_X, w, Constants.WILD_SLOTX_WILD, count,ngramCount);
			}
			else if (type.equals(Constants.P_SLOTX_WILD)) {
				if (arr.length != P_SLOTX_WILD_LENGTH) {L.log("Bad length (" + arr.length + ")" + value); return;}
				String p = arr[1];
				String w = arr[4];
				String count = arr[8];
				String ngramCount = arr[7];
				
				double countDouble = 0.0;
				try {
					countDouble = Double.parseDouble(count);
				}catch(Exception e){}
				
				if (countDouble > DPMinCount) {
					emit(context, p, Constants.SLOT_X, w, Constants.P_SLOTX_WILD, count,ngramCount);
				}
			}
			else if (type.equals(Constants.WILD_SLOTX_W1)) {
				if (arr.length != WILD_SLOTX_W1_LENGTH) {L.log("Bad length (" + arr.length + ")" + value); return;}
				String p = arr[6];
				String w = arr[4];
				String count = arr[8];
				String ngramCount = arr[7];
				emit(context, p, Constants.SLOT_X, w, Constants.WILD_SLOTX_W1, count,ngramCount);
			}
			else if (type.equals(Constants.P_SLOTY_W2)) {
				if (arr.length != P_SLOTY_W2_LENGTH) {L.log("Bad length (" + arr.length + ")" + value); return;}
				String p = arr[1];
				String w = arr[3];
				String count = arr[5];
				String ngramCount = arr[4];
				emit(context, p, Constants.SLOT_Y, w, Constants.P_SLOTY_W2, count,ngramCount);
			}
			else if (type.equals(Constants.WILD_SLOTY_WILD)) {
				if (arr.length != WILD_SLOTY_WILD_LENGTH) {L.log("Bad length  (" + arr.length + ")" + arr.length + ")" + value); return;}
				String p = arr[6];
				String w = arr[5];
				String count = arr[8];
				String ngramCount = arr[7];
				emit(context, p, Constants.SLOT_Y, w, Constants.WILD_SLOTY_WILD, count,ngramCount);
			}
			else if (type.equals(Constants.P_SLOTY_WILD)) {
				if (arr.length != P_SLOTY_WILD_LENGTH) {L.log("Bad length (" + arr.length + ")" + value); return;}
				String p = arr[1];
				String w = arr[5];
				String ngramCount = arr[7];
				String count = arr[8];
				
				double countDouble = 0.0;
				try {
					countDouble = Double.parseDouble(count);
				}catch(Exception e){}
				
				if (countDouble > DPMinCount) {
					emit(context, p, Constants.SLOT_Y, w, Constants.P_SLOTY_WILD, count,ngramCount);
				}
			}
			else if (type.equals(Constants.WILD_SLOTY_W2)) {
				if (arr.length != WILD_SLOTY_W2_LENGTH) {L.log("Bad length (" + arr.length + ")" + value); return;}
				String p = arr[6];
				String w = arr[3];
				String ngramCount = arr[7];
				String count = arr[8];
				emit(context, p, Constants.SLOT_Y, w, Constants.WILD_SLOTY_W2, count,ngramCount);
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
			
			// Each of these is corresponding to the type number.
			String T1 = null, T2 = null, T3 = null, T4 = null, T5 = null, T6 = null, T7 = null, T8 = null;
			String[] arr;
			String type, ngramCount = "";
			for (Text value : values) {
				arr = value.toString().trim().split(Constants.S);
				if (arr.length != 3) {
					L.log("Bad situation MICalculatorReducer: " + key + " , " + value);
					continue;
				}
				type = arr[0].trim();
				if (type.equals(Constants.P_SLOTX_W1)) {
					T1 = arr[1];
					ngramCount = arr[2];
				}
				else if (type.equals(Constants.WILD_SLOTX_WILD)) T2 = arr[1];
				else if (type.equals(Constants.P_SLOTX_WILD))	 T3 = arr[1];
				else if (type.equals(Constants.WILD_SLOTX_W1)) 	 T4 = arr[1];
				else if (type.equals(Constants.P_SLOTY_W2)) {
					T5 = arr[1];
					ngramCount = arr[2];
				}
				else if (type.equals(Constants.WILD_SLOTY_WILD)) T6 = arr[1];
				else if (type.equals(Constants.P_SLOTY_WILD)) 	 T7 = arr[1];
				else if (type.equals(Constants.WILD_SLOTY_W2))	 T8 = arr[1];
			}
			
			boolean weHaveFullSlotXParams = T1 != null && T2 != null && T3 != null && T4 != null;
			boolean weHaveFullSlotYParams = T5 != null && T6 != null && T7 != null && T8 != null;
			
			if (!weHaveFullSlotXParams && !weHaveFullSlotYParams) {
				L.log("We do not have enough params to calculate! - " + key + ", " + values);
				return;
			}
			
			double mi = 0.0, tfidf = 0.0, dice = 0.0;
			if (weHaveFullSlotXParams) {
				mi = PaperHuristics.calculateMI(T1, T2, T3, T4);
				tfidf = MeniHueristics.calculateTFIDF(T1, T4);
				dice = MeniHueristics.calculateDice(T1, T4, T3);
			}
			else {
				mi = PaperHuristics.calculateMI(T5, T6, T7, T8);
				tfidf = MeniHueristics.calculateTFIDF(T5, T8);
				dice = MeniHueristics.calculateDice(T5, T8, T7);
			}
			
			// Key = <p, slot, w>
			KEY.set(key.toString() + Constants.S + ngramCount + Constants.S + mi + Constants.S + tfidf + Constants.S + dice);
			VAL.set("");
			context.write(KEY, VAL);
		}
	}
}
