package mapreduce;

import huristics.PaperHuristics;

import java.io.IOException;
import java.security.Key;

import model.TripleEntry;
import model.TripleSlotEntry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import Utils.Constants;
import Utils.DLogger;

public class TripleDatabaseManufactor {
	
	final static DLogger L = new DLogger(true, "TripleDatabaseManufactor");
	final static String[] Ps = {"X akhtar Y" + Constants.S + "X akhund Y"};
	final static int MAPPER_INPUT_LENGTH	= 5;
	
	/**
	 * 
	 * Input - @see MICalculatorReducer. Can accept any of it's outputs.
	 *  	1. <p, slotX, w1, count, mi>
	 *  	2. <p, slotY, w2, count, mi>
	 * 	
	 * Output - 
	 * 		<p1, p2> -> <slot, w, count, mi>
	 * 		One line of out for each pair that contains this p.
	 * 
	 * @author asaf
	 *
	 */
	public static class TripleDatabaseManufactorMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		// Objects for reuse
		private final static Text Key = new Text();
		private final static Text Val = new Text();
				
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] arr = value.toString().trim().split(Constants.S);
			
			if (arr.length != MAPPER_INPUT_LENGTH) {
				L.log("Something bad happened - " + value);
				return;
			}
			
			String p = arr[0];
			for (String pair : Ps) {
				int index = pair.indexOf(p);
				if (index != -1) {
					Key.set(pair);
					Val.set(value);
					context.write(Key, Val);
				}
			}
		}
	}
	
	/**
	 * 
	 * Input - @see TripleDatabaseManufactorMapper. Can accept any of it's outputs.
	 * 			<p1, p2> -> <p, slot, w, count, mi>
	 * Output - 
	 * 		    <p1, p2> -> sim
	 * 	
	 * @author asaf
	 *
	 */
	public static class TripleDatabaseManufactorReducer extends
			Reducer<Text, Text, Text, Text> {
		// Reuse objects
		private final static Text KEY = new Text();
		private final static Text VAL = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			String[] arr = key.toString().trim().split(Constants.S);
			if (arr.length != 2) {
				L.log("Bad situation - " + key);
				return;
			}
			
			String p1 = arr[0].trim(), p2 = arr[1].trim(), p=null, word=null, slot=null;
			double mi = 0.0;
			long count = 0;
			TripleEntry tripleEntry = null;
			TripleEntry p1Entry = new TripleEntry(p1);
			TripleEntry p2Entry = new TripleEntry(p2);
			TripleSlotEntry slotEntry = null;
			
			for (Text value : values) {
				arr = value.toString().trim().split(Constants.S);
				if (arr.length != MAPPER_INPUT_LENGTH) {
					L.log("Bad Length = " + value);
					continue;
				}
				
				try {
					count = (long) Double.parseDouble(arr[3].trim());
					mi = Double.parseDouble(arr[4].trim());
				}catch(Exception e) {
					L.log("Failed to parse line = " + value + ", arr[3] = " + arr[3] + ", arr[4] = " + arr[4] + e);
				}
				
				p = arr[0].trim();
				slot = arr[1].trim();
				word = arr[2];
				slotEntry = new TripleSlotEntry(word, count, mi);
				
				tripleEntry = p.equals(p1) ? p1Entry : p2Entry;
				if (slot.equals(Constants.SLOT_X)) {
					tripleEntry.addSlotX(slotEntry);
				}
				else {
					tripleEntry.addSlotY(slotEntry);
				}
			}
			
			double sim = PaperHuristics.calculateSim(p1Entry, p2Entry);
			KEY.set(key);
			VAL.set(String.valueOf(sim));
			context.write(KEY, VAL);
		}
	}
}
