package task1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.S3Credentials;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.umd.cloud9.io.pair.PairOfStrings;

public class Main {

	private static double minPmi = Double.MAX_VALUE;
	private static double relMinPmi = Double.MAX_VALUE;
	
	private static final Logger LOG = Logger.getAnonymousLogger();

	private static final String TMP_FILE_PATH_1 = "/user/hduser/ass_2_intermediate_1"; //"s3n://mevuzarot.task2/intermediate/1";// Used for the c() fumction file - first job
	private static final String TMP_FILE_PATH_2 = "/user/hduser/ass_2_intermediate_2"; //"s3n://mevuzarot.task2/intermediate/2";// used for the npmi calculation file - second job
	private static final String TMP_FILE_PATH_0 = "/user/hduser/ass_2_intermediate_0"; //"s3n://mevuzarot.task2/intermediate/0";// used for the npmi calculation file - second job
	private static final String HDFS_FIRST_SPLIT_SUFFIX = "/part-r-00000";
	
	private static final int LEFT = 1;
	private static final int RIGHT = 2;

	
	/**************************
	 * 
	 * Decade merge
	 * 
	 * Input - 2gram
	 * output - [w1 w2 decade] -> count
	 */
	private static class DecadeMergeMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		// Objects for reuse
		private final static TextArrayWritable KEY = new TextArrayWritable();
		private final static Text TextKey = new Text();
		private final static Text W1 = new Text();
		private final static Text W2 = new Text();
		private final static Text DECADE = new Text();
		private final static IntWritable COUNT = new IntWritable();
		private final static Text ARRAY[] = new Text[3];

		private static final String LEFT_PREFIX = "left_";
		private static final String RIGHT_PREFIX = "rigt_";


		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] arr = value.toString().trim().split("\\s+");
			if (!AppearanceCountMapper.validateInput2gram(arr)) {
				return;
			}
			
			W1.set(arr[0]);
			W2.set(arr[1]);
			DECADE.set(arr[2].substring(0, 3)); // 1998 - > 199
			ARRAY[0] = W1;
			ARRAY[1] = W2;
			ARRAY[2] = DECADE;
			
			KEY.set(ARRAY); 
			COUNT.set(Integer.parseInt(arr[3]));
			TextKey.set(KEY.toString());
			context.write(TextKey, COUNT);
		}
	}

	/**
	 * 
	 * Input - [w1 w2 decade] -> count
	 * Output w1 w2 decade count
	 * 
	 * @author asaf
	 *
	 */
	private static class DecadeMergeReducer extends
			Reducer<Text, IntWritable, Text, Text> {
		// Reuse objects

		private final static Text VAL = new Text();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
			long sum = 0;
			for (IntWritable i : values) {
				sum += i.get();
			}
			
			VAL.set(String.valueOf(sum));
			context.write(key,VAL);
		}
	}
	
	/**************************
	 * 
	 * Appearance counting
	 * 
	 * Input - w1 w2 year count
	 * output - w1(side + year) -> count w1 w2
	 */
	private static class AppearanceCountMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		// Objects for reuse
		private final static Text KEY = new Text();
		private final static Text VAL = new Text();
		private final static IntWritable COUNT = new IntWritable();

		private static final String LEFT_PREFIX = "left_";
		private static final String RIGHT_PREFIX = "righ_";

		private static String appendLeftPrefix(String word) {
			if (word != null) {
				return LEFT_PREFIX + word;
			}

			return null;
		}

		private static String appendRightPrefix(String word) {
			if (word != null) {
				return RIGHT_PREFIX + word;
			}

			return null;
		}
		
		public static String appendCentury(String word, String year) throws IOException {
			if (word != null && year != null && year.length() >= 3) {
				return  year.substring(0, 3) + "_" + word;
			}
			
			throw new IOException(
					"Bad words for append century ::" + word + ", " + year);
		}
		
		public static String removeCenturyPrefix(String word) throws IOException {
			if (word != null && word.length() >= 4) {
				return word.substring(4);
			}
	
			throw new IOException(
					"Bad words for remove century ::" + word);
		}
		
		private static boolean validateInput2gram(String[] lineArray) {
			 if(lineArray.length >= 4 && lineArray[2].length() >= 4) {
				 return true;
			 }
			 else {
				 //	LOG.info("Bad input for mapper --> (" + value + ")::(" + arr.length + ")");
				 return false;
			 }
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] arr = value.toString().trim().split("\\s+");

			KEY.set(appendLeftPrefix(appendCentury(arr[0], arr[2]))); //Left_198_w1
			VAL.set(arr[3] + " " + arr[0] + " " + arr[1]); // count w1 w2
			context.write(KEY, VAL);
			KEY.set(appendRightPrefix(appendCentury(arr[1], arr[2]))); // Right_198_w2
			context.write(KEY, VAL);
			
			//LOG.info("1::    " + value.toString() + "//// output - " + KEY+ " :: "  + VAL);
		}
	}

	/**
	 * 
	 * Input - w(side + year) count w2 w3
	 * Output (multiple) w(side + year) sum count w2 w3 
	 * 
	 * @author asaf
	 *
	 */
	private static class AppearanceCountReducer extends
			Reducer<Text, Text, Text, Text> {
		// Reuse objects
		private final static IntWritable SUM = new IntWritable();
		private final static Text VAL = new Text();
		private final static Set<String> CACHE = new HashSet<String>();

		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			String[] arr;
			CACHE.clear();
			for (Text value : values) {
				arr = value.toString().trim().split("\\s+");
				sum += Integer.parseInt(arr[0]);
				CACHE.add(value.toString());
			}
			// 1::    """ tonal"	2006	52	48	37//// output - left_200_tonal" :: 52 """ tonal"
			// left_188_( :::: 1 ( Dravidian ::: 1 ( Dravidian
			//right_199_plus :::: 29 $900 plus ::: 95 29 $900 plus
			//right_199_plus :::: 43 $16.50 plus ::: 95 43 $16.50 plus
			//right_199_plus :::: 23 $16 plus ::: 95 23 $16 plus

			for (String str : CACHE) {
				VAL.set(sum + " " + str);
				context.write(key, VAL);
				//System.out.println(key + " :::: " + str + " ::: " + VAL);
			}			
		}
	}
	
	/************************
	 * 
	 * Pairs PMI calculation
	 * 
	 * Input -  w(side + year) sum count w2 w3 
	 * Output -  (w2 + w3 + decade) -> count w2-sum w3-sum
	 * 
	 * @author asaf
	 *
	 */

	private static class PairsPMIMapper extends
			Mapper<LongWritable, Text, PairOfStrings, Text> {

		// Objects for reuse
		private final static PairOfStrings PAIR = new PairOfStrings();
		private final static Text VAL = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] arr = value.toString().trim().split("\\s+");
			//in -left_190_$50	45 15 3 $50 gain
			//out - (190_$50 , gain) -> left 15 3
			String left = null;
			String right = null, decade = null;
			//(_89_1981, _1981) righ
			if (arr[0].startsWith("l") ) {
				left = arr[0].substring(9, arr[0].length());
				right = arr[5];
				decade = arr[0].substring(5, 8);
			} else {
				right = arr[0].substring(9, arr[0].length());
				left = arr[5];
				decade = arr[0].substring(5, 8);
				//System.out.println("Right - " + arr[0] + ": right:" +right + ",  Left:" + left + "  , decade = " + decade);
			}
			
			PAIR.set(decade + "_" + left, right);
			VAL.set(arr[0].substring(0, 4)+ " " + arr[2] + " " + arr[3]);
			context.write(PAIR, VAL);
			
			//System.out.println("Mapper - " + PAIR + "::::" + VAL );
			
			//check it out here, for some reason the right pair are fucked up. (both pairs of the same word)
			//Also fix the last job to be accurate.
		}
	}

	/*
	// Combiner
	private static class PairsPMICombiner extends
			Reducer<PairOfStrings, Text, PairOfStrings, IntWritable> {
		private static IntWritable SUM = new IntWritable();

		@Override
		public void reduce(PairOfStrings pair, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (Text value : values) {
				sum += value.get();
			}
			SUM.set(sum);
			context.write(pair, SUM);
		}
	}
	*/

	/**
	 * Input -   (w2 + w3 + decade) -> count w2-sum w3-sum
	 * Output -  decade w2 w3 pmi
	 * 
	 * @author asaf
	 *
	 */
	// Second Stage reducer: Finalizes PMI Calculation given
	private static class PairsPMIReducer extends
			Reducer<PairOfStrings, Text, Text, Text> {

		// TODO: set the number of items in the heb/eng corpus.
		private static double totalDocs = 156215.0;////////////////////////////////////////////////
		// Objects for reuse
		private final static Text KEY = new Text();
		private final static Text VAL = new Text();

		@Override
		public void reduce(PairOfStrings pair, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			//(190_$50 , gain) -> left 15 3
			int pairSum = 0;
			String last = "";
			double rightOcc = -1, leftOcc = -1;
			
			for (Text value : values) {
				//System.out.println("PMIIN -> " + pair + " >>> " + value);
				
				String[] arr = value.toString().trim().split("\\s+");
				if (arr.length < 3) {
					System.out.println("Bad array - " + value);
					continue;
				}
				
				pairSum = Integer.parseInt(arr[2]);
				if (arr[0].startsWith("l")) {
					leftOcc = Integer.parseInt(arr[1]);
				}
				else if (arr[0].startsWith("r")) {
					rightOcc = Integer.parseInt(arr[1]);
				}
				else {
					System.out.println("Error - not starting with l or r - " + value);
				}
				
				last = value.toString();
			}
			
			if(rightOcc == -1 || leftOcc == -1) {
				System.out.println("Bad seatuation - " + last + "---"+ rightOcc+ ":" + leftOcc +"::::::" + pair + ">>> " );
				return;
			}
			
			// Look up individual totals for each member of pair
			// Calculate PMI emit Pair or Text as key and Float as value
			String left = pair.getLeftElement();
			String right = pair.getRightElement();

			String leftWithPre = AppearanceCountMapper
					.appendLeftPrefix(left);
			String rightWithPre = AppearanceCountMapper
					.appendRightPrefix(right);
			
			if (leftWithPre == null || rightWithPre == null) {
				LOG.info("Null after removing prefix - (" + leftWithPre + ":" + rightWithPre + ")");
				return;
			}
			
			double probPair = pairSum / totalDocs;
			double probLeft = leftOcc / totalDocs;
			double probRight =  rightOcc / totalDocs;

			double pmi = Math.log(probPair / (probLeft * probRight));
			double npmi = pmi / (-Math.log(probPair));
			
			KEY.set(left.substring(0, 3)); // The decade
			VAL.set(AppearanceCountMapper
					.removeCenturyPrefix(left) + " " + right + " " + npmi);
			context.write(KEY, VAL);
			
		}
	}
	
	/**************************
	 * 
	 * Pmi Filter
	 * 
	 * Input - decade w2 w3 pmi
	 * Output - (decade) -> w2 w3 pmi
	 * 
	 */
	private static class PmiFilterMapper extends
			Mapper<LongWritable, Text, Text, PairOfStrings> {

		// Objects for reuse
		private final static Text KEY = new Text();
		private final static PairOfStrings STR = new PairOfStrings();

		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] arr = value.toString().trim().split("\\s+");
			KEY.set(arr[0]); // The decade
			STR.set(arr[1] + " " + arr[2], arr[3]);
			context.write(KEY, STR);
		}
	}
	
	/**
	 * Input - (decade) -> w2 w3 pmi
	 * Output - w2 w3 pmi (only ones who passed the filter).
	 * 
	 * @author asaf
	 *
	 */
	private static class PmiFilterReducer extends
			Reducer<Text, PairOfStrings, PairOfStrings, Text> {
		// Reuse objects
		// Objects for reuse
		private final static PairOfStrings PAIR = new PairOfStrings();
		private final static Text VAL = new Text();

		
		
		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			minPmi = Double.parseDouble(conf.get("minPmi"));
			relMinPmi = Double.parseDouble(conf.get("relMinPmi"));
		}
		
		@Override
		public void reduce(Text key, Iterable<PairOfStrings> values,
				Context context) throws IOException, InterruptedException {
			double totalPmiInDecade = 0;
			String[] arr = null;
			PairOfStrings pair;
			Iterator<PairOfStrings> it = values.iterator();
			Set<PairOfStrings> cache = new HashSet<PairOfStrings>();
			while (it.hasNext()) {
				pair = it.next();
				double npmi = Double.parseDouble(pair.getRightElement());
				totalPmiInDecade += npmi;
				cache.add(pair);
			}

			for (PairOfStrings p : cache) {
				arr = p.getLeftElement().trim().split("\\s+");
				double npmi = Double.parseDouble(p.getRightElement());
				if (npmi >  minPmi || (npmi / totalPmiInDecade) > relMinPmi) {
					PAIR.set(arr[0], arr[1]);
					VAL.set(p.getRightElement());
					context.write(PAIR, VAL);
				}	
			}
		}
	}
	public static void main(String[] args) throws Exception {
		
		if (args.length < 2) {
			LOG.info("Please provide at least two arguments.");
			System.exit(0);
		}
		
		String inputPath = args[0];

		// TODO This output path is for the 2nd job's.
		// The fits job will have an intermediate output path from which the
		// second job's reducer will read
		String outputPath = args[1];
		String intermediatePath1 = TMP_FILE_PATH_1;
		String intermediatePath2 = TMP_FILE_PATH_2;
		String intermediatePath0 = TMP_FILE_PATH_0;

		LOG.info("Tool: Appearances Part");
		LOG.info(" - input path: " + inputPath);
		LOG.info(" - output path: " + intermediatePath1);
		
		Configuration conf = new Configuration();
		conf.set("intermediatePath1", intermediatePath1);
		conf.set("intermediatePath2", intermediatePath2);
		conf.set("intermediatePath0", intermediatePath0);
		
		//conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
		//conf.set("fs.s3n.awsAccessKeyId",Credentials.AWS_ACCESS);
		//conf.set("fs.s3n.awsSecretAccessKey",Credentials.AWS_SECRET);
		//conf.set("fs.default.name","s3n://dsp132/heb-2gram-10K");
		
		conf.set("minPmi", args[2]);
		conf.set("relMinPmi", args[3]);
		
		boolean usingStopWords = args[4].equals("1");
		conf.setBoolean("usingStopWords", new Boolean(usingStopWords));
		
		conf.set("total_input_items_count", args[5]);
		
		// Second job
		Job job0 = Job.getInstance(conf);
		job0.setJobName("DecadeMergeCounter");
		job0.setJarByClass(Main.class);
		job0.setMapperClass(DecadeMergeMapper.class);
		job0.setReducerClass(DecadeMergeReducer.class);

		job0.setInputFormatClass(SequenceFileInputFormat.class);
		// Set Output and Input Parameters
	    job0.setOutputKeyClass(Text.class);
	    job0.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job0, new Path(args[0]));
		FileOutputFormat.setOutputPath(job0, new Path(intermediatePath0));
		
		Path outputDir0 = new Path(intermediatePath0);
		FileSystem.get(conf).delete(outputDir0, true);
		
		long startTime = System.currentTimeMillis();
		boolean status = job0.waitForCompletion(true);
		LOG.info("PairsPmiCounter Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		System.exit(0);
		
		Job job1 = Job.getInstance(conf);
		job1.setJobName("AppearanceCount");
		job1.setJarByClass(Main.class);

		FileInputFormat.setInputPaths(job1, new Path(intermediatePath0));
		FileOutputFormat.setOutputPath(job1, new Path(intermediatePath1));

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setMapperClass(AppearanceCountMapper.class);
		job1.setCombinerClass(AppearanceCountReducer.class);
		job1.setReducerClass(AppearanceCountReducer.class);

		// Delete the output directory if it exists already.
		Path intermediateDir = new Path(intermediatePath1);
		FileSystem.get(conf).delete(intermediateDir, true);

		 startTime = System.currentTimeMillis();
		job1.waitForCompletion(true);
		LOG.info("Apperance Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		
		// Second job
		Job job2 = Job.getInstance(conf);
		job2.setJobName("PairsPmiCounter");
		job2.setJarByClass(Main.class);
		job2.setMapperClass(PairsPMIMapper.class);
		job2.setReducerClass(PairsPMIReducer.class);

		job2.setOutputKeyClass(PairOfStrings.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job2, new Path(intermediatePath1));
		FileOutputFormat.setOutputPath(job2, new Path(intermediatePath2));
		
		Path outputDir = new Path(intermediatePath2);
		FileSystem.get(conf).delete(outputDir, true);
		
		startTime = System.currentTimeMillis();
		status = job2.waitForCompletion(true);
		LOG.info("PairsPmiCounter Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		System.exit(0);
		
		// Third job
		Job job3 = Job.getInstance(conf);
		job3.setJobName("Pmi Filter");
		job3.setJarByClass(Main.class);
		job3.setMapperClass(PmiFilterMapper.class);
		job3.setReducerClass(PmiFilterReducer.class);

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(PairOfStrings.class);	
		
		FileInputFormat.addInputPath(job3, new Path(intermediatePath2));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		
		// Delete the output directory if it exists already.
		//Path outDir = new Path(outputPath);
		//FileSystem.get(conf).delete(outDir, true);

		startTime = System.currentTimeMillis();
		status = job3.waitForCompletion(true);
		LOG.info("PmiFilter Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		System.exit(status ? 0 : 1);
	}
}