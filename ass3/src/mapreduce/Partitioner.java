package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import Utils.Constants;

public class Partitioner {

	/**
	 * This class makes sure that all duplicated keys will get to the same reducer.
	 * @author asaf
	 *
	 */
	public static class DuplicateKeysPartitioner extends HashPartitioner<Text, Text> {
		
		private static final Text TMP = new Text();
		
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String str = key.toString();
			Text KEY = key;
			if (str.substring(str.length() - 1).equals(Constants.LOWEST_ASCII)) {
				// This is a secondary key
				TMP.set(str.substring(0, str.length() - 1));
				KEY = TMP;
			}
			
			return super.getPartition(KEY, value, numReduceTasks);
	    }
	}

}
