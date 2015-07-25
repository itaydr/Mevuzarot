package mapreduce;

import java.io.IOException;

import model.NGram;
import model.NGramFactory;
import Utils.Constants;
import Utils.DLogger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MIInfoExtractor {

	final static DLogger L = new DLogger(true);
	
	/**************************
	 * 
	 * This mapper is incharge of multiple mappings - 
	 * 1. <p, slotX, w1> -> <count>		[4]
	 * 2. <p, slotY, w2> -> <Count>		[4]
	 * 3. <*, slotX, *> -> <Ngram>		[7]
	 * 4. <*, slotY, *> -> <NGram>		[7]
	 * 5. <p, slotX, *> -> <Ngram>		[7]
	 * 6. <p, slotY, *> -> <NGram>		[7]
	 * 7. <*, slotX, w1> -> <Ngram>		[7]
	 * 8. <*, slotY, w2> -> <NGram>		[7]
	 * 
	 * 
	 * Input is the initial ngram input files.
	 */
	public static class MIInfoExtractorMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		// Objects for reuse
		private final static Text Key = new Text();
		private final static Text Val = new Text();
		
		@Override
		public void setup(Context context) throws IOException {

		}
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			NGram ngram = NGramFactory.parseNGram(value.toString());
			if (ngram == null) {
				return;
			}
			
			emitSlotX(context, ngram); // 1
			emitSlotY(context, ngram); // 2
			emitSlotCount(context, ngram, Constants.SLOT_X); // 3 
			emitSlotCount(context, ngram, Constants.SLOT_Y); // 4
			emitWordCountInSlotForPath(context, ngram, Constants.SLOT_X); // 5
			emitWordCountInSlotForPath(context, ngram, Constants.SLOT_Y); // 6
			emitPathCountForSlotAndWord(context, ngram, Constants.SLOT_X, ngram.slotX); // 7
			emitPathCountForSlotAndWord(context, ngram, Constants.SLOT_Y, ngram.slotY); // 8
		}
		
		/**
		 * Emits <P, SlotX, W1> -> <count>
		 * @param ngram
		 */
		private void emitSlotX(Context context, NGram ngram) throws IOException, InterruptedException{
			String key = ngram.path + Constants.S + Constants.SLOT_X + Constants.S + ngram.slotX;
			emitDuplicatedLine(context, key, String.valueOf(ngram.count));
		}
		
		/**
		 * Emits <P, SlotY, W2> -> <count>
		 * @param ngram
		 */
		private void emitSlotY(Context context, NGram ngram) throws IOException, InterruptedException{
			String key = ngram.path + Constants.S + Constants.SLOT_Y + Constants.S + ngram.slotY;
			emitDuplicatedLine(context, key, String.valueOf(ngram.count));
		}
		
		/**
		 * Emits <*, Slot, *> -> <w1, w2, p, count>
		 * @param ngram
		 */
		private void emitSlotCount(Context context, NGram ngram, String slotIdentifier) throws IOException, InterruptedException{
			String key =  Constants.WILDCARD + Constants.S + slotIdentifier + Constants.S + Constants.WILDCARD;
			emitDuplicatedLine(context, key, ngram.toString());
		}
		
		/**
		 * Emits  <p, slot, *> -> <Ngram>
		 * @param ngram
		 */
		private void emitWordCountInSlotForPath(Context context, NGram ngram, String slotIdentifier) throws IOException, InterruptedException{
			String key =  ngram.path + Constants.S + slotIdentifier + Constants.S + Constants.WILDCARD;
			emitDuplicatedLine(context, key, ngram.toString());
		}
		
		/**
		 * Emits   <*, slot, w1> -> <Ngram>
		 * @param ngram
		 */
		private void emitPathCountForSlotAndWord(Context context, NGram ngram, String slotIdentifier, String word) throws IOException, InterruptedException{
			String key =  Constants.WILDCARD + Constants.S + slotIdentifier + Constants.S + word;			
			emitDuplicatedLine(context, key, ngram.toString());
		}
		
		/**
		 * Emits   <*, slot, w1> -> <Ngram>
		 * @param ngram
		 */
		private void emitDuplicatedLine(Context context, String key, String val) throws IOException, InterruptedException{
			Key.set(key);
			Val.set(val);
			context.write(Key, Val);
			
			// Emit extra
			Key.set(key+Constants.LOWEST_ASCII);
			context.write(Key, Val);
		}
	}
	
	/**
	 * 
	 * Input - @see MIInfoExtractorMapper. Can accept any of it's outputs.
	 * 
	 * Output - Basically we out put the original data received by each line, with the full
	 * 			count for the key, appended to it by a seperator. 
	 * 	
	 * @note - we assume that the last object in the Val received from the mapper is always the count.
	 * 
	 * @author asaf
	 *
	 */
	public static class MIInfoExtractorReducer extends
			Reducer<Text, Text, Text, Text> {
		// Reuse objects

		private final static Text KEY = new Text();
		private final static Text VAL = new Text();
		private static long sum = 0;

		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			if (sum == 0) {
				for (Text value : values) {
					String valStr = value.toString();
					String arr[] = valStr.trim().split(Constants.S);
					double count = 0;
					try {
					count = Double.parseDouble(arr[arr.length-1]);
					} catch (Exception e) {L.log("Failed to parse the count for line : " + value); }
					sum += count; 
				}
			}
			else {
				
				for (Text value : values) {
					String finalKey = key.toString() + Constants.S + value.toString() + Constants.S + String.valueOf(sum); 
					KEY.set(finalKey);
					VAL.set("");
					context.write(KEY,VAL);
				}
				sum = 0;
			}
		}
	}
	
	
}
