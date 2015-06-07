package task1;

import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

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
	/*
	private static final String TMP_FILE_PATH_1 = "s3n://mevuzarot.task2/intermediate/11";//"/user/hduser/ass_2_intermediate_1"// Used for the c() fumction file - first job
	private static final String TMP_FILE_PATH_2 = "s3n://mevuzarot.task2/intermediate/22";//"/user/hduser/ass_2_intermediate_2" used for the npmi calculation file - second job
	private static final String TMP_FILE_PATH_0 = "s3n://mevuzarot.task2/intermediate/00";//"/user/hduser/ass_2_intermediate_0"// used for the npmi calculation file - second job
	private static final String TMP_FILE_DECADE_BIGRAM_COUNT = "s3n://mevuzarot.task2/intermediate/decade_bigram_count0";//"/user/hduser/ass_2_intermediate_decade_bigram_count"// used for the npmi calculation file - second job
	*/
	///*
	private static final String TMP_FILE_PATH_1 = "/user/hduser/ass_2_intermediate_1";// Used for the c() fumction file - first job
	private static final String TMP_FILE_PATH_2 = "/user/hduser/ass_2_intermediate_2";// used for the npmi calculation file - second job
	private static final String TMP_FILE_PATH_0 = "/user/hduser/ass_2_intermediate_0";// used for the npmi calculation file - second job
	private static final String TMP_FILE_DECADE_BIGRAM_COUNT = "/user/hduser/ass_2_intermediate_decade_bigram_count";// used for the npmi calculation file - second job
	//*/
	
	private static final String LEFT = "l";
	private static final String RIGHT = "r";
	private static final String S = " ";
	private static final String LOWEST_ASCII = "	"; // TAB = 9
	
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
		private static boolean usingEnglish;
		private static boolean usingStopWords;
		private static int count = 50;
		
		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			usingEnglish = conf.getBoolean("usingEnglish", true);
			usingStopWords = conf.getBoolean("usingStopWords", false);
		}
		
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
			
			String w1 = arr[0];
			String w2 = arr[1];
			
			if (usingStopWords && (StopWords.isStopWord(w1, usingEnglish) || StopWords.isStopWord(w2, usingEnglish))) {
				return;
			}
			
			W1.set(w1);
			W2.set(w2);
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
	 * Decade merge
	 * 
	 * Input - w1 w2 decade count
	 * output - [decade] -> w1 w2 count
	 */
	private static class DecadeBigramCountMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		// Objects for reuse
		private final static Text KEY = new Text();
		private final static Text VAL = new Text();


		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] arr = value.toString().trim().split("\\s+");
			
			// First
			KEY.set(arr[2]); // decade
			VAL.set(arr[0] + S + arr[1] + S + arr[3]);
			context.write(KEY, VAL);
			
			// Second
			KEY.set(arr[2] + LOWEST_ASCII); // decade
			context.write(KEY, VAL);
		}
	}
	
	/**
	 * This class makes sure that all duplicated keys will get to the same reducer.
	 * @author asaf
	 *
	 */
	private class MatchingDuplicateKeysPartitioner extends HashPartitioner<Text, Text> {
		
		private final Text TMP = new Text();
		
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String str = key.toString();
			Text KEY = key;
			if (str.substring(str.length() - 1).equals(LOWEST_ASCII)) {
				// This is a secondary key
				TMP.set(str.substring(0, str.length() - 1));
				KEY = TMP;
			}
			
			return super.getPartition(KEY, value, numReduceTasks);
	    }
	}

	/**
	 * 
	 * Input - [decade] -> w1 w2 count
	 * Output w1 w2 decade count totalDecade
	 * 
	 * TODO: make sure the partitioner send both keys to the same reducer.
	 * 
	 * @author asaf
	 *
	 */
	private static class DecadeBigramCountReducer extends
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
					String arr[] = valStr.trim().split("\\s+");
					int count = Integer.parseInt(arr[2]);
					sum += count;
				}
			}
			else {
				
				String decade = key.toString();
				for (Text value : values) {
					String arr[] = value.toString().trim().split("\\s+");
					KEY.set(arr[0]);
					VAL.set(arr[1] + S + decade + S + arr[2] + S + String.valueOf(sum));
					context.write(KEY,VAL);
				}
				sum = 0;
			}
		}
	}
	
	
	/**************************
	 * 
	 *  1 word Appearance counting
	 * 
	 * Input - w1 w2 decade count totalDecade
	 * output - <left, w1 ,decade> -> w2 count 
	 */
	private static class AppearanceCountMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		// Objects for reuse
		private final static Text KEY = new Text();
		private final static Text VAL = new Text();
				
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

			
			 //* Input - w1 w2 decade count decadeCount
			 //* output - <w1, left, decade> -> w2 count decadeCount
			 
			 
			String[] arr = value.toString().trim().split("\\s+");
			String w1 = arr[0];
			String w2 = arr[1];
			String decade = arr[2];
			String count = arr[3];
			
			KEY.set(LEFT + S + w1 + S + decade); // l w1 decade decadeCount
			VAL.set(w2 + S + count); // w2 count
			context.write(KEY, VAL);
			// Write the same live for iteration purposes.
			KEY.set(LEFT + S + w1 + S + decade + LOWEST_ASCII);
			context.write(KEY, VAL);
			
			VAL.set(w1 + S + count); // w1 count
			KEY.set(RIGHT + S + w2 + S + decade); // r w2 decade
			context.write(KEY, VAL);
			//
			KEY.set(RIGHT + S + w2 + S + decade + LOWEST_ASCII); // r w2 decade
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
		private static long sum = 0;

		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			String[] arr;
			if (sum == 0) {
				for (Text value : values) {
					arr = value.toString().trim().split("\\s+");
					sum += Integer.parseInt(arr[1]);
				}
			}
			else {	
				for (Text value : values) {
					VAL.set(value.toString() + S + sum);
					context.write(key, VAL);
				}	
				
				sum = 0;
			}
		}
	}
	
	/************************
	 * 
	 * Pairs PMI calculation
	 *  @itay- this is not the correct input.
	 * Input -  a. w1 w2 decade count decadeCount
	 * 			b. left w1 w2 decade count  sum
	 * 			c. right w1 w2 decade count sum - maybe order of words is different.
	 * Output -  <w1, w2, decade> -> 
	 * 			a. count decadeCount
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
			String w1, w2, decade, count, decadeCount;
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
				decadeCount = arr[5];
				VAL.set(RIGHT + S + count);
			}
			else {
				// 2-gram
				w1 = arr[0];
				w2 = arr[1];
				decade = arr[2];
				count = arr[3];
				decadeCount = arr[4];
				VAL.set(count + S + decadeCount);
			}

			KEY.set(w1 + S + w2 + S + decade);
			context.write(KEY, VAL);
		}
	}
	
	/**
	 * Input -   <w1, w2, decade> - >
	 * 									a. count decadeCount
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

		// Objects for reuse
		private final static Text KEY = new Text();
		private final static Text VAL = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			String keyArr[] = key.toString().trim().split("\\s+");
			int leftSum = -1, rightSum = -1, count = -1;
			double decadeCount = -1;
			for (Text value : values) {
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
					decadeCount = Integer.parseInt(arr[1]);
				}
			}
						
			if(leftSum == -1 || rightSum == -1 || count == -1 || decadeCount == -1 || decadeCount == 0) {
				System.out.println("Bad situation ----("+ leftSum+ ":" + rightSum + ":" + decadeCount + ")::::::>>>>>>>" + key + "<<<<<<<<<< " );
				return;
			}

			double probPair = count / decadeCount;
			double probLeft = leftSum / decadeCount;
			double probRight =  rightSum / decadeCount;

			double pmi = Math.log(probPair / (probLeft * probRight));
			double npmi = pmi / (-Math.log(probPair));
			/*
			System.out.println("word =" + key + " decadeCount= " + decadeCount
					+ ", count= " + count
					+ ", leftSum=" + leftSum + ", rightSum=" + rightSum
					+ ", probPair :" + probPair + ", probLeft = " + probLeft + 
					", probRight :" + probRight + ", pmi :" + pmi
					+ ", npmi :" + npmi);
			*/
			
			//System.out.println("PMI - pair - " + probPair + ", left= " + probLeft + ", " + probRight + ", totalDocs = "+ totalDocs + ", pmi = "+ pmi + ", npmi = " + npmi);
			
			KEY.set(keyArr[0] + S + keyArr[1]); // w1
			VAL.set(keyArr[2] +  S + npmi + S + pmi);
			
			context.write(KEY, VAL);
		}
	}
	
	/**************************
	 * 
	 * Pmi Filter
	 * 
	 * Input -  w1 w2 decade npmi pmi
	 * Output - (decade) -> w1 w2 npmi pmi
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
			VAL.set(arr[0] + S + arr[1] + S + arr[3] + S + arr[4]);
			context.write(KEY, VAL);
			
			KEY.set(arr[2] + LOWEST_ASCII);
			context.write(KEY, VAL);
		}
	}
	
	/**
	 * Input - (decade) -> w1 w2 npmi pmi
	 * Output - decade npmi pmi w1 w2 (only ones who passed the filter).
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
		private static double totalPmiInDecade = 0;
		
		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			minPmi = Double.parseDouble(conf.get("minPmi"));
			relMinPmi = Double.parseDouble(conf.get("relMinPmi"));
			
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			String[] arr = null;
			//System.out.println("Reducer key =" + key + "=, sum = " + totalPmiInDecade);
			
			if (totalPmiInDecade == 0) {
				for (Text value : values) {
					arr = value.toString().trim().split("\\s+");
					double npmi = Double.parseDouble(arr[2]);
					totalPmiInDecade += npmi;
				}
			}
			else {
				int i = 0;
				int j = 0;
				for (Text value : values) {
					arr = value.toString().trim().split("\\s+");
					double npmi = Double.parseDouble(arr[2]);
					if (npmi >  minPmi || (npmi / totalPmiInDecade) > relMinPmi) {
						KEY.set(key.toString()); // decade
						VAL.set(String.valueOf(npmi) + S + arr[3] + S +arr[0] + S +arr[1]); // npmi pmi w1 w2
						context.write(KEY, VAL);
						j++;
					}
					i++;
				}
				
				//System.out.println("Reducer " + key + ", count= "+ i + ", filtered = "+ j
					//	+ ", total = " + totalPmiInDecade);
				
				totalPmiInDecade = 0;
			}
		}
	}
	/**
	 * Filters text of format <decade pmi>
	 * @author asaf
	 *
	 */
	public static class FilterComparable extends WritableComparator {
	    
		private final static Text T = new Text();
		private final static Text T2 = new Text();
		@Override
	    public int compare(WritableComparable o,WritableComparable o2){
	        System.out.println("in compare");
	        Text t = (Text)o;
	        Text t2 = (Text)o2;
	        
	        String arr1[] = t.toString().trim().split("\\s+");
	        String arr2[] = t.toString().trim().split("\\s+");
	        
	        if (arr1.length != 2 || arr2.length != 2) {
	        	System.out.println("Ban lenghts in compare "+ t + S + t2);
	        	return t.compareTo(t2);
	        }
	        
	        T.set(arr1[0]);
	        T2.set(arr2[0]);
	        
	        // Reverse order.
	        int res = T2.compareTo(T);
	        if (res != 0) {
	        	return res;
	        }
	        else {
	        	T.set(arr1[1]);
	        	T2.set(arr2[1]);
	        	return T2.compareTo(T);
	        }
	    }
	}
	
	 /** A WritableComparator optimized for Text keys. */
	  public static class Comparator extends WritableComparator {
	    public Comparator() {
	      super(Text.class);
	    }
	    
	    
	    @Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
	    	System.out.println("Inside "+ a +S+b);
			return super.compare(a, b);
		}



		public int compare(byte[] b1, int s1, int l1,
	                       byte[] b2, int s2, int l2) {
	      
	    
		  int n1 = 1;//WritableUtils.decodeVIntSize(b1[s1]);
	      int n2 = 2;//WritableUtils.decodeVIntSize(b2[s2]);
	      return compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2);
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
		boolean isRunningInCloud = args[5].equals("1");
		conf.setBoolean("usingStopWords", usingStopWords);
		conf.setBoolean("usingEnglish", args[6].equals("eng"));	
		
		// Second job
		Job mergeDecadesJob = Job.getInstance(conf);
		mergeDecadesJob.setJobName("DecadeMergeCounter");
		mergeDecadesJob.setJarByClass(Main.class);
		mergeDecadesJob.setMapperClass(DecadeMergeMapper.class);
		mergeDecadesJob.setReducerClass(DecadeMergeReducer.class);

		//mergeDecadesJob.setInputFormatClass(SequenceFileInputFormat.class);
		// Set Output and Input Parameters
		mergeDecadesJob.setOutputKeyClass(Text.class);
		mergeDecadesJob.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(mergeDecadesJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(mergeDecadesJob, new Path(intermediatePath0));
		
		if (!isRunningInCloud) {
			Path outputDir0 = new Path(intermediatePath0);
			FileSystem.get(conf).delete(outputDir0, true);
		}
		long startTime = System.currentTimeMillis();
		boolean status = mergeDecadesJob.waitForCompletion(true);
		LOG.info("PairsPmiCounter Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		// Second job
		Job decade2gramCountJob = Job.getInstance(conf);
		decade2gramCountJob.setJobName("DecadeSumCounter");
		decade2gramCountJob.setJarByClass(Main.class);
		decade2gramCountJob.setMapperClass(DecadeBigramCountMapper.class);
		decade2gramCountJob.setReducerClass(DecadeBigramCountReducer.class);
		decade2gramCountJob.setPartitionerClass(MatchingDuplicateKeysPartitioner.class);
		
		// Set Output and Input Parameters
		decade2gramCountJob.setOutputKeyClass(Text.class);
		decade2gramCountJob.setOutputValueClass(Text.class);
				
		FileInputFormat.addInputPath(decade2gramCountJob, new Path(intermediatePath0));
		FileOutputFormat.setOutputPath(decade2gramCountJob, new Path(TMP_FILE_DECADE_BIGRAM_COUNT));
				
		if (!isRunningInCloud) {
			Path outputDirBigramCount = new Path(TMP_FILE_DECADE_BIGRAM_COUNT);
			FileSystem.get(conf).delete(outputDirBigramCount, true);
		}
		startTime = System.currentTimeMillis();
		status = decade2gramCountJob.waitForCompletion(true);
		LOG.info("Decade sum counter Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		
		Job c1c2CounterJob = Job.getInstance(conf);
		c1c2CounterJob.setJobName("AppearanceCount");
		c1c2CounterJob.setJarByClass(Main.class);

		FileInputFormat.setInputPaths(c1c2CounterJob, new Path(intermediatePath0));
		FileOutputFormat.setOutputPath(c1c2CounterJob, new Path(intermediatePath1));

		c1c2CounterJob.setOutputKeyClass(Text.class);
		c1c2CounterJob.setOutputValueClass(Text.class);

		c1c2CounterJob.setMapperClass(AppearanceCountMapper.class);
		c1c2CounterJob.setReducerClass(AppearanceCountReducer.class);

		// Delete the output directory if it exists already.
		if (!isRunningInCloud) {
			Path intermediateDir = new Path(intermediatePath1);
			FileSystem.get(conf).delete(intermediateDir, true);
		}

		 startTime = System.currentTimeMillis();
		 c1c2CounterJob.waitForCompletion(true);
		LOG.info("Apperance Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");		
		
		// Second job
		Job pmiCalculatorJob = Job.getInstance(conf);
		pmiCalculatorJob.setJobName("PairsPmiCounter");
		pmiCalculatorJob.setJarByClass(Main.class);
		pmiCalculatorJob.setMapperClass(PairsPMIMapper.class);
		pmiCalculatorJob.setReducerClass(PairsPMIReducer.class);

		pmiCalculatorJob.setOutputKeyClass(Text.class);
		pmiCalculatorJob.setOutputValueClass(Text.class);
		
		// Read from 2 files!
		FileInputFormat.setInputPaths(pmiCalculatorJob, new Path(TMP_FILE_DECADE_BIGRAM_COUNT), new Path(intermediatePath1));
		FileOutputFormat.setOutputPath(pmiCalculatorJob, new Path(intermediatePath2));
		
		if (!isRunningInCloud) {
			Path outputDir = new Path(intermediatePath2);
			FileSystem.get(conf).delete(outputDir, true);
		}
		
		startTime = System.currentTimeMillis();
		status = pmiCalculatorJob.waitForCompletion(true);
		LOG.info("PairsPmiCounter Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		// Third job
		Job pmiFilterJob = Job.getInstance(conf);
		pmiFilterJob.setJobName("Pmi Filter");
		pmiFilterJob.setJarByClass(Main.class);
		pmiFilterJob.setMapperClass(PmiFilterMapper.class);
		pmiFilterJob.setReducerClass(PmiFilterReducer.class);
		pmiFilterJob.setPartitionerClass(MatchingDuplicateKeysPartitioner.class);
		
		pmiFilterJob.setOutputFormatClass(TextOutputFormat.class);
		pmiFilterJob.setMapOutputKeyClass(Text.class);
		pmiFilterJob.setMapOutputValueClass(Text.class);	
		
		FileInputFormat.addInputPath(pmiFilterJob, new Path(intermediatePath2));
		FileOutputFormat.setOutputPath(pmiFilterJob, new Path(outputPath));
		
		// Delete the output directory if it exists already.
		if (!isRunningInCloud) {
			Path outDir = new Path(outputPath);
			FileSystem.get(conf).delete(outDir, true);
		}

		startTime = System.currentTimeMillis();
		status = pmiFilterJob.waitForCompletion(true);
		LOG.info("PmiFilter Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		System.exit(status ? 0 : 1);
	}
}