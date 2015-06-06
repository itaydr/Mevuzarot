package task1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * Need to calculate the N for each decade. totalDocs will not do it!
 * 
 * 
 * 
 * @author asaf
 *
 */


public class Main {

	private static double minPmi = Double.MAX_VALUE;
	private static double relMinPmi = Double.MAX_VALUE;
	
	private static final Logger LOG = Logger.getAnonymousLogger();

	private static final String TMP_FILE_PATH_1 = "/user/hduser/ass_2_intermediate_1"; //"s3n://mevuzarot.task2/intermediate/1";// Used for the c() fumction file - first job
	private static final String TMP_FILE_PATH_2 = "/user/hduser/ass_2_intermediate_2"; //"s3n://mevuzarot.task2/intermediate/2";// used for the npmi calculation file - second job
	private static final String TMP_FILE_PATH_0 = "/user/hduser/ass_2_intermediate_0"; //"s3n://mevuzarot.task2/intermediate/0";// used for the npmi calculation file - second job
	private static final String HDFS_FIRST_SPLIT_SUFFIX = "/part-r-00000";

	private static final String LEFT = "l";
	private static final String RIGHT = "r";
	private static final String S = " ";
	
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


		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] arr = value.toString().trim().split("\\s+");
			if (!AppearanceCountMapper.validateInput2gram(arr)) {
				return;
			}
			
			//Configuration conf = context.getConfiguration();
			//if (conf.get("one") == null) {
			//	conf.set("one", "1");
			//}
			//else {
			//	return;
			//}
			
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
			
			//TODO:
			//COUNT.set(COUNT.get() + 2);
			//context.write(TextKey, COUNT);
			//ARRAY[0] = new Text("HELLO");
			//KEY.set(ARRAY);
			//TextKey.set(KEY.toString());
			//context.write(TextKey, COUNT);
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
	 *  1 word Appearance counting
	 * 
	 * Input - w1 w2 decade count
	 * output - <left, w1 ,decade> -> w2 count
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

			
			 //* Input - w1 w2 decade count
			 //* output - <w1, left, decade> -> w2 count
			 
			 
			String[] arr = value.toString().trim().split("\\s+");
			String w1 = arr[0];
			String w2 = arr[1];
			String decade = arr[2];
			String count = arr[3];
			
			KEY.set(LEFT + S + w1 + S + decade); // l w1 decade
			VAL.set(w2 + S + count); // w2 count
			context.write(KEY, VAL);
			VAL.set(w1 + S + count); // w1 count
			KEY.set(RIGHT + S + w2 + S + decade); // r w2 decade
			context.write(KEY, VAL);
			
			//LOG.info("1::    " + value.toString() + "//// output - " + KEY+ " :: "  + VAL);
		}
	}

	/**
	 * 
	 * Input -  <left, w1, decade> -> w2 count
	 * Output (multiple) left w1 w2 decade count sum
	 * 
	 * @author asaf
	 *
	 */
	private static class AppearanceCountReducer extends
			Reducer<Text, Text, Text, Text> {
		// Reuse objects
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
				sum += Integer.parseInt(arr[1]);
				CACHE.add(value.toString());
			}
			
			for (String str : CACHE) {
				VAL.set(str + S + sum);
				context.write(key, VAL);
				//System.out.println(key + " :::: " + str + " ::: " + VAL);
			}			
		}
	}
	
	/************************
	 * 
	 * Pairs PMI calculation
	 *  @itay- this is not the correct input.
	 * Input -  a. 2-grams.
	 * 			b. left w1 w2 decade count sum
	 * 			c. right w1 w2 decade count sum - maybe order of words is different.
	 * Output -  <w1, w2, decade> -> 
	 * 			a. count.
	 * 			b. l sum
	 * 			c. r sum
	 * 
	 * @author asaf
	 *
	 */

	private static class PairsPMIMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		// Objects for reuse
		private final static Text VAL = new Text();
		private final static Text KEY = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//System.out.println("PMI mapper in - " + key + ":" + value);
			String[] arr = value.toString().trim().split("\\s+");
			String first = arr[0]; // this is served as the type for left/right lines.
			String w1, w2, decade, count;
			// TODO: check if has 3 slots.
			if (first.equals(LEFT)) {
				//left w1 w2 decade count sum
				w1 = arr[1];
				w2 = arr[3];
				decade = arr[2];
				count = arr[5];
				VAL.set(LEFT + S + count);
			}
			else if (first.equals(RIGHT)) {
				// right w1 w2 decade count sum
				w1 = arr[3];
				w2 = arr[1];
				decade = arr[2];
				count = arr[5];
				VAL.set(RIGHT + S + count);
			}
			else {
				// 2-gram
				w1 = arr[0];
				w2 = arr[1];
				decade = arr[2].substring(0, 3);
				count = arr[3];
				VAL.set(count);
			}
			
			/**
			 * 
			 * PMI reducer - """ 200 shies":l 3, count = 1, self = task1.Main$PairsPMIReducer@1c92025
PMI reducer - """ shies" 200:3, count = 1, self = task1.Main$PairsPMIReducer@1c92025
PMI reducer - 3 200 """:r shies", count = 1, self = task1.Main$PairsPMIReducer@1c92025

			 * 
			 * PMI reducer - """ 200 shies":l 3, count = 1, self = task1.Main$PairsPMIReducer@643d12f
PMI reducer - """ shies" 200:3, count = 1, self = task1.Main$PairsPMIReducer@643d12f
PMI reducer - shies" 200 """:r 3, count = 1, self = task1.Main$PairsPMIReducer@643d12f

			 * 
			 */
			
			//System.out.println("PMI mapper - " + KEY + ":" + VAL);
			
			KEY.set(w1 + S + w2 + S + decade);
			context.write(KEY, VAL);
		}
	}
	
	/**
	 * Input -   <w1, w2, decade> - >
	 * 									a. count
	 * 									b. l count
	 * 									c. r count
	 * Output -  w1 w2 decade pmi
	 * 
	 * @author asaf
	 *
	 */
	// Second Stage reducer: Finalizes PMI Calculation given
	private static class PairsPMIReducer extends
			Reducer<Text, Text, Text, Text> {

		// TODO: set the number of items in the heb/eng corpus.
		private static double totalDocs = 1000.0;////////////////////////////////////////////////
		// Objects for reuse
		private final static Text KEY = new Text();
		private final static Text VAL = new Text();
		private final List<Text> CACHE = new ArrayList<Text>();

		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			Text last = null;
			CACHE.clear();
			int i = 0;
			for (Text value : values) {
				//System.out.println("pmi reduce cache key = " + key + ", val = " + value);
				last = value;
				CACHE.add(new Text (value));
				//System.out.println("PMI - val =" + value +", Cache - " + CACHE);
				i++;
			}
			
			// Must have 3 lines to calculate pmi
			if (i != 3) {
				System.out.println("PMI reducer - " + key +":"+ last + ", count = " + i + ", self = " + this);
				for (Text value : CACHE)  {
					context.write(key, value);
				}
				return;
				//throw new IOException("Less than 3 lines in pmi reducer :" + i + ":::" + last);
			}
			
			//System.out.println("PMI - Cache - " + CACHE);
			
			String keyArr[] = key.toString().trim().split("\\s+");
			int leftSum = -1, rightSum = -1, count = -1;
			for (Text value : CACHE) {
				String[] arr = value.toString().trim().split("\\s+");
				String first = arr[0]; // this is served as the type for left/right lines.
				
				if (first.equals(LEFT)) {
					leftSum = Integer.valueOf(arr[1]);
				}
				else if (first.equals(RIGHT)) {
					rightSum = Integer.valueOf(arr[1]);
				}
				else {
					count = Integer.valueOf(arr[0]);
				}
				
				//System.out.println("PMI reducer - key=" + key +": value="+ value + ", l = " + leftSum + ", r=" + rightSum +", c=" + count +  ", self = " + this);
				
			}
						
			if(leftSum == -1 || rightSum == -1 || count == -1) {
				System.out.println("Bad seatuation ----"+ leftSum+ ":" + rightSum +"::::::" + key + ">>> " );
				return;
			}

			double probPair = count / totalDocs;
			double probLeft = leftSum / totalDocs;
			double probRight =  rightSum / totalDocs;

			double pmi = Math.log(probPair / (probLeft * probRight));
			double npmi = pmi / (-Math.log(probPair));
			
			//System.out.println("PMI - pair - " + probPair + ", left= " + probLeft + ", " + probRight + ", totalDocs = "+ totalDocs + ", pmi = "+ pmi + ", npmi = " + npmi);
			
			KEY.set(keyArr[0] + S + keyArr[1]); // w1
			VAL.set(keyArr[2] +  S + npmi);
			
			context.write(KEY, VAL);
		}
	}
	
	/**************************
	 * 
	 * Pmi Filter
	 * 
	 * Input -  w1 w2 decade pmi
	 * Output - (decade) -> w1 w2 pmi
	 * 
	 */
	private static class PmiFilterMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		// Objects for reuse
		private final static Text KEY = new Text();
		private final static Text VAL = new Text();

		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] arr = value.toString().trim().split("\\s+");
			KEY.set(arr[2]); // The decade
			VAL.set(arr[0] + S + arr[1] + S + arr[3]);
			context.write(KEY, VAL);
		}
	}
	
	/**
	 * Input - (decade) -> w1 w2 pmi
	 * Output - w2 w3 pmi (only ones who passed the filter).
	 * 
	 * @author asaf
	 *
	 */
	private static class PmiFilterReducer extends
			Reducer<Text, Text, Text, Text> {
		// Reuse objects
		// Objects for reuse
		private final static Text KEY = new Text();
		private final static Text VAL = new Text();
		private final static Set<Text> CACHE = new HashSet<Text>();

		
		
		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			minPmi = Double.parseDouble(conf.get("minPmi"));
			relMinPmi = Double.parseDouble(conf.get("relMinPmi"));
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			double totalPmiInDecade = 0;
			String[] arr = null;
			Text pair;
			Iterator<Text> it = values.iterator();
			CACHE.clear();
			while (it.hasNext()) {
				pair = it.next();
				arr = pair.toString().trim().split("\\s+");
				double npmi = Double.parseDouble(arr[2]);
				totalPmiInDecade += npmi;
				CACHE.add(new Text(pair));
			}

			for (Text p : CACHE) {
				arr = p.toString().trim().split("\\s+");
				double npmi = Double.parseDouble(arr[2]);
				if (npmi >  minPmi || (npmi / totalPmiInDecade) > relMinPmi) {
					KEY.set(arr[0] + S +arr[1]);
					VAL.set(String.valueOf(npmi));
					context.write(KEY, VAL);
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
				
		conf.set("minPmi", args[2]);
		conf.set("relMinPmi", args[3]);
		
		boolean usingStopWords = args[4].equals("1");
		boolean isRunningInCloud = args.length >= 7 && args[6].equals("1");
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
		
		if (!isRunningInCloud) {
			Path outputDir0 = new Path(intermediatePath0);
			FileSystem.get(conf).delete(outputDir0, true);
		}
		long startTime = System.currentTimeMillis();
		boolean status = job0.waitForCompletion(true);
		LOG.info("PairsPmiCounter Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
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
		if (!isRunningInCloud) {
			Path intermediateDir = new Path(intermediatePath1);
			FileSystem.get(conf).delete(intermediateDir, true);
		}

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

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		// Read from 2 files!
		FileInputFormat.setInputPaths(job2, new Path(intermediatePath0), new Path(intermediatePath1));
		FileOutputFormat.setOutputPath(job2, new Path(intermediatePath2));
		
		if (!isRunningInCloud) {
			Path outputDir = new Path(intermediatePath2);
			FileSystem.get(conf).delete(outputDir, true);
		}
		
		startTime = System.currentTimeMillis();
		status = job2.waitForCompletion(true);
		LOG.info("PairsPmiCounter Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		// Third job
		Job job3 = Job.getInstance(conf);
		job3.setJobName("Pmi Filter");
		job3.setJarByClass(Main.class);
		job3.setMapperClass(PmiFilterMapper.class);
		job3.setReducerClass(PmiFilterReducer.class);

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);	
		
		FileInputFormat.addInputPath(job3, new Path(intermediatePath2));
		FileOutputFormat.setOutputPath(job3, new Path(outputPath));
		
		// Delete the output directory if it exists already.
		if (!isRunningInCloud) {
			Path outDir = new Path(outputPath);
			FileSystem.get(conf).delete(outDir, true);
		}

		startTime = System.currentTimeMillis();
		status = job3.waitForCompletion(true);
		LOG.info("PmiFilter Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		System.exit(status ? 0 : 1);
	}
}