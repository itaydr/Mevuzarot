package mapreduce;

import java.io.IOException;

import model.NGram;
import model.NGramFactory;
import Utils.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MIInfoExtractor {

	/**************************
	 * 
	 * This mapper is incharge of multiple mappings - 
	 * 1. <p, slotX, w1> -> <count>
	 * 2. <p, slotY, w2> -> <Count>
	 * 3. <*, slotX, *> -> <Ngram>
	 * 4. <*, slotY, *> -> <NGram>
	 * 5. <p, slotX, *> -> <Ngram>
	 * 6. <p, slotY, *> -> <NGram>
	 * 7. <*, slotX, w1> -> <Ngram>
	 * 8. <*, slotY, w2> -> <NGram>
	 * 
	 * 
	 * Input is the initial ngram input files.
	 */
	private static class MIInfoExtractorMapper extends
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
}
