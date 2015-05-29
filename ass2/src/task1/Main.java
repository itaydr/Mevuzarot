package task1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

	private static final String TMP_FILE_PATH_1 = "/user/hduser/ass_2_intermediate_1"; // Used for the c() fumction file - first job
	private static final String TMP_FILE_PATH_2 = "/user/hduser/ass_2_intermediate_2"; // used for the npmi calculation file - second job
	private static final String HDFS_FIRST_SPLIT_SUFFIX = "/part-r-00000";
	private static final String DECADE_SUM_PREFIX = "decade_";

	private static BigInteger ENG_BIGRAM_COUNT = new BigInteger("6626604215");
	private static BigInteger HEB_BIGRAM_COUNT = new BigInteger("252069581");

	/**************************
	 * 
	 * Appearance counting
	 * 
	 * 
	 */
	private static class AppearanceCountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		// Objects for reuse
		private final static Text KEY = new Text();
		private final static IntWritable COUNT = new IntWritable();

		private static final String LEFT_PREFIX = "left_";
		private static final String RIGHT_PREFIX = "right_";

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
		
		private static String appendCentury(String word, String year) throws IOException {
			if (word != null && year != null && year.length() > 3) {
				return  year.substring(0, 3) + "_" + word;
			}
			
			throw new IOException(
					"Bad words for append century ::" + word + ", " + year);
		}
		
		private static String removeCenturyPrefix(String word) throws IOException {
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
			if (!validateInput2gram(arr)) {
			return;
			}
			
			COUNT.set(Integer.parseInt(arr[3]));
			KEY.set(appendLeftPrefix(appendCentury(arr[0], arr[2])));
			context.write(KEY, COUNT);
			KEY.set(appendLeftPrefix(appendCentury(arr[1], arr[2])));
			context.write(KEY, COUNT);
		}
	}

	// First stage Reducer: Totals counts for each Token and Token Pair
	private static class AppearanceCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		// Reuse objects
		private final static IntWritable SUM = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			SUM.set(sum);
			context.write(key, SUM);
		}
	}
	
	/************************
	 * 
	 * Pairs PMI calculation
	 * 
	 * @author asaf
	 *
	 */

	private static class PairsPMIMapper extends
			Mapper<LongWritable, Text, PairOfStrings, IntWritable> {

		// Objects for reuse
		private final static PairOfStrings PAIR = new PairOfStrings();
		private final static IntWritable COUNT = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] arr = value.toString().trim().split("\\s+");
			if (!AppearanceCountMapper.validateInput2gram(arr)) {
				return;
			}
			
			PAIR.set(AppearanceCountMapper.appendCentury(arr[0], arr[2]), 
					AppearanceCountMapper.appendCentury(arr[1], arr[2]));
			COUNT.set(Integer.parseInt(arr[3]));
			context.write(PAIR, COUNT);
		}
	}

	// Combiner
	private static class PairsPMICombiner extends
			Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
		private static IntWritable SUM = new IntWritable();

		@Override
		public void reduce(PairOfStrings pair, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			SUM.set(sum);
			context.write(pair, SUM);
		}
	}

	// Second Stage reducer: Finalizes PMI Calculation given
	private static class PairsPMIReducer extends
			Reducer<PairOfStrings, IntWritable, Text, Text> {

		private static Map<String, Integer> termTotals = new HashMap<String, Integer>();

		private static DoubleWritable PMI = new DoubleWritable();
		// TODO: set the number of items in the heb/eng corpus.
		private static double totalDocs = 156215.0;
		// Objects for reuse
		private final static Text KEY = new Text();
		private final static Text VAL = new Text();

		@Override
		public void setup(Context context) throws IOException {
			// TODO Read from intermediate output of first job
			// and build in-memory map of terms to their individual totals
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);

			// Path inFile = new Path(conf.get("intermediatePath"));
			Path inFile = new Path(TMP_FILE_PATH_1 + HDFS_FIRST_SPLIT_SUFFIX);

			if (!fs.exists(inFile)) {
				throw new IOException("File Not Found: " + inFile.toString());
			}

			BufferedReader reader = null;
			try {
				FSDataInputStream in = fs.open(inFile);
				InputStreamReader inStream = new InputStreamReader(in);
				reader = new BufferedReader(inStream);

			} catch (FileNotFoundException e) {
				throw new IOException(
						"Exception thrown when trying to open file.");
			}

			String line = reader.readLine();
			while (line != null) {

				String[] parts = line.split("\\s+");
				if (parts.length != 2) {
					LOG.info("Input line did not have exactly 2 tokens: '"
							+ line + "'");
				} else {
					termTotals.put(parts[0], Integer.parseInt(parts[1]));
				}
				line = reader.readLine();
			}
			
			LOG.info("\n\n\n\nTerm totals length = " + termTotals.size() + "\n\n\n\n");

			reader.close();

		}

		@Override
		public void reduce(PairOfStrings pair, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// Recieving pair and pair counts -> Sum these for this pair's total
			// Only calculate PMI for pairs that occur 10 or more times
			int pairSum = 0;
			for (IntWritable value : values) {
				pairSum += value.get();
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

			if (termTotals == null) {
				LOG.info("Term total in null");
				return;
			}
			
			double probPair = pairSum / totalDocs;
			double leftOcc = termTotals.containsKey(leftWithPre) ? termTotals.get(leftWithPre) : 1;
			double probLeft = leftOcc / totalDocs;
			double rightOcc =  termTotals.containsKey(rightWithPre) ? termTotals.get(rightWithPre) : 1;
			double probRight =  rightOcc / totalDocs;

			double pmi = Math.log(probPair / (probLeft * probRight));
			double npmi = pmi / (-Math.log(probPair));

			double sumInDecade = 1;
			
			//if (npmi > minPmi || (npmi / sumInDecade) >= relMinPmi) {
				pair.set(AppearanceCountMapper
						.removeCenturyPrefix(left), AppearanceCountMapper
						.removeCenturyPrefix(right));

				PMI.set(npmi);
				KEY.set(left.substring(0, 3));
				VAL.set(pair.getLeftElement() + " " + pair.getRightElement() + " " + npmi);
				context.write(KEY, VAL);
			//}
		}
	}
	
	/**************************
	 * 
	 * Appearance counting
	 * 
	 * 
	 */
	private static class PmiFilterMapper extends
			Mapper<LongWritable, Text, Text, PairOfStrings> {

		// Objects for reuse
		private final static Text KEY = new Text();
		private final static Text VAL = new Text();
		private final static PairOfStrings STR = new PairOfStrings();

		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] arr = value.toString().trim().split("\\s+");
			KEY.set(arr[0]); // The decade
			VAL.set(arr[1] + " " + arr[2]+ " " + arr[3]); // Rest of the info
			
			//LOG.info("Last map value --- " + KEY + ":: " + VAL);
			STR.set(arr[1] + " " + arr[2], arr[3]);
			context.write(KEY, STR);//VAL);
		}
	}
	
	// First stage Reducer: Totals counts for each Token and Token Pair
	private static class PmiFilterReducer extends
			Reducer<Text, PairOfStrings, PairOfStrings, Text> {
		// Reuse objects
		// Objects for reuse
		private final static PairOfStrings PAIR = new PairOfStrings();
		private final static Text VAL = new Text();

		@Override
		public void reduce(Text key, Iterable<PairOfStrings> values,
				Context context) throws IOException, InterruptedException {
			double totalPmiInDecade = 0;
			String[] arr = null;
			PairOfStrings pair;
			Iterator<PairOfStrings> it = values.iterator();
			List<PairOfStrings> cache = new ArrayList<PairOfStrings>();
			while (it.hasNext()) {
				pair = it.next();
				double npmi = Double.parseDouble(pair.getRightElement());
				totalPmiInDecade += npmi;
				cache.add(pair);
			}

			for (PairOfStrings p : cache) {
				arr = p.getLeftElement().trim().split("\\s+");
				double npmi = Double.parseDouble(p.getRightElement());
				if (npmi > minPmi || (npmi / totalPmiInDecade) > relMinPmi) {
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

		LOG.info("Tool: Appearances Part");
		LOG.info(" - input path: " + inputPath);
		LOG.info(" - output path: " + intermediatePath1);

		Configuration conf = new Configuration();
		conf.set("intermediatePath1", intermediatePath1);
		conf.set("intermediatePath2", intermediatePath2);
		minPmi = Double.parseDouble(args[2]);
		relMinPmi = Double.parseDouble(args[3]);
		
		// TODO: fetch from conf
		conf.set("minPmi", args[2]);
		conf.set("relMinPmi", args[3]);

		Job job1 = Job.getInstance(conf);
		job1.setJobName("AppearanceCount");
		job1.setJarByClass(Main.class);

		FileInputFormat.setInputPaths(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(intermediatePath1));

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setInputFormatClass(SequenceFileInputFormat.class);

		job1.setMapperClass(AppearanceCountMapper.class);
		job1.setCombinerClass(AppearanceCountReducer.class);
		job1.setReducerClass(AppearanceCountReducer.class);

		// Delete the output directory if it exists already.
		Path intermediateDir = new Path(intermediatePath1);
		FileSystem.get(conf).delete(intermediateDir, true);

		long startTime = System.currentTimeMillis();
		job1.waitForCompletion(true);
		LOG.info("Apperance Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		// Second job
		Job job2 = Job.getInstance(conf);
		job2.setJobName("PairsPmiCounter");
		job2.setJarByClass(Main.class);
		job2.setMapperClass(PairsPMIMapper.class);
		job2.setCombinerClass(PairsPMICombiner.class);
		job2.setReducerClass(PairsPMIReducer.class);

		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputKeyClass(PairOfStrings.class);
		job2.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(intermediatePath2));
		
		Path outputDir = new Path(intermediatePath2);
		FileSystem.get(conf).delete(outputDir, true);
		
		startTime = System.currentTimeMillis();
		boolean status = job2.waitForCompletion(true);
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
		job3.setMapOutputValueClass(PairOfStrings.class);	
		
		FileInputFormat.addInputPath(job3, new Path(intermediatePath2));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		
		// Delete the output directory if it exists already.
		Path outDir = new Path(outputPath);
		FileSystem.get(conf).delete(outDir, true);

		startTime = System.currentTimeMillis();
		status = job3.waitForCompletion(true);
		LOG.info("PmiFilter Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		System.exit(status ? 0 : 1);
	}
}