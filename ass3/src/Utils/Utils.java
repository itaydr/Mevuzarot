package Utils;

import org.apache.hadoop.fs.Path;

public class Utils {
	
	private final static int NUMBER_OF_INPUT_PATHS = 10;
	
	public static Path[] generateInputPaths() {
		Path[] paths = new Path[NUMBER_OF_INPUT_PATHS];
		
		for (int i = 0 ; i < NUMBER_OF_INPUT_PATHS ; i++) {
			String p = "s3n://dsp152/syntactic-ngram/biarcs/biarcs." + String.format("%02d", i) +"-of-99";
			paths[i] = new Path(p);
		}
		
		return paths;
	}
}
