package main;

import huristics.PaperHuristics;
import mapreduce.MICalculator.MICalculatorMapper;
import mapreduce.MICalculator.MICalculatorReducer;
import mapreduce.MIInfoExtractor.MIInfoExtractorMapper;
import mapreduce.MIInfoExtractor.MIInfoExtractorReducer;
import mapreduce.TripleDatabaseManufactor.TripleDatabaseManufactorMapper;
import mapreduce.TripleDatabaseManufactor.TripleDatabaseManufactorReducer;
import model.NGram;
import model.NGramFactory;
import model.TripleEntry;
import model.TripleSlotEntry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Utils.DLogger;

public class JobConstructor {

	final static DLogger L = new DLogger(true, "JobConstructor");
	private final static int MIN_INPUT_ARGS_COUNT = 2;

	
	  private static String MIINFO_EXTRACTOR_INTERMEDIATE_PATH =
	  "s3n://mevuzarot.task2/intermediate/mi_info_extractor";
	private static String MI_CALCULATOR_INTERMEDIATE_PATH =
			"s3n://mevuzarot.task2/intermediate/mi_calculator";
	private static String TRIPLE_DATABASE_INTERMEDIATE_PATH = "s3n://mevuzarot.task2/intermediate/triple_database";

	private static final String MIINFO_EXTRACTOR_INTERMEDIATE_PATH_LOCAL = "/user/hduser/mi_info_extractor";
	private static final String MI_CALCULATOR_INTERMEDIATE_PATH_LOCAL = "/user/hduser/mi_calculator";
	private static final String TRIPLE_DATABASE_INTERMEDIATE_PATH_LOCAL = "/user/hduser/triple_database";

	
	/**
	 * Input - 0. Input path. 1. Output path. 2. DPMinCount 3. MinFeatureNum 4.
	 * Is running in cloud
	 * 
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		if (args.length < MIN_INPUT_ARGS_COUNT) {
			L.log("Please provide at least two arguments.");
			System.exit(0);
		}

		String inputPath = args[0];
		String outputPath = args[1];
		String miInforExtractorIntermediatePath = MIINFO_EXTRACTOR_INTERMEDIATE_PATH;
		String miCalculatorIntemediatePath = MI_CALCULATOR_INTERMEDIATE_PATH;
		String tripleDatabaseIntermediatePath = TRIPLE_DATABASE_INTERMEDIATE_PATH;
		
		L.log("JobCostructor start:");
		L.log(" - input path: " + inputPath);
		L.log(" - output path: " + outputPath);
		L.log(" - MIInfoExtractor path: " + miInforExtractorIntermediatePath);
		L.log(" - MICalculator path: " + miCalculatorIntemediatePath);
		L.log(" - TripleDatabase path: " + tripleDatabaseIntermediatePath);
		
		Configuration conf = new Configuration();

		conf.set("DPMinCount", args[2]);
		conf.set("MinFeatureNum", args[3]);

		boolean isRunningInCloud = args[4].equals("1");

		if (!isRunningInCloud) {
			miInforExtractorIntermediatePath = MIINFO_EXTRACTOR_INTERMEDIATE_PATH_LOCAL;
			miCalculatorIntemediatePath = MI_CALCULATOR_INTERMEDIATE_PATH_LOCAL;
			tripleDatabaseIntermediatePath = TRIPLE_DATABASE_INTERMEDIATE_PATH_LOCAL;
		}
		
		long startTime;
		boolean status;

		// First Job
		Job miInfoExtractorJob = Job.getInstance(conf);
		miInfoExtractorJob.setJobName("miInfoExtractorJob");
		miInfoExtractorJob.setJarByClass(JobConstructor.class);
		miInfoExtractorJob.setMapperClass(MIInfoExtractorMapper.class);
		miInfoExtractorJob
				.setPartitionerClass(mapreduce.Partitioner.DuplicateKeysPartitioner.class);
		miInfoExtractorJob.setReducerClass(MIInfoExtractorReducer.class);

		// Set Output and Input Parameters
		//if (isRunningInCloud) {
			miInfoExtractorJob.setInputFormatClass(SequenceFileInputFormat.class);
		//}
		miInfoExtractorJob.setOutputKeyClass(Text.class);
		miInfoExtractorJob.setOutputValueClass(Text.class);

		Path[] paths = Utils.Utils.generateInputPaths();
		if (!isRunningInCloud) {
			paths = new Path[] {new Path(inputPath)};
		}
		
		FileInputFormat.setInputPaths(miInfoExtractorJob,paths);
		FileOutputFormat.setOutputPath(miInfoExtractorJob, new Path(
				miInforExtractorIntermediatePath));

		if (!isRunningInCloud) {
			Path outputDir0 = new Path(miInforExtractorIntermediatePath);
			FileSystem.get(conf).delete(outputDir0, true);
		}
		startTime = System.currentTimeMillis();
		status = miInfoExtractorJob.waitForCompletion(true);
		L.log("miInfoExtractorJob Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		// Second job
		Job miCalculatorJob = Job.getInstance(conf);
		miCalculatorJob.setJobName("miCalculatorJob");
		miCalculatorJob.setJarByClass(JobConstructor.class);
		miCalculatorJob.setMapperClass(MICalculatorMapper.class);
		miCalculatorJob.setReducerClass(MICalculatorReducer.class);

		// Set Output and Input Parameters
		miCalculatorJob.setOutputKeyClass(Text.class);
		miCalculatorJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(miCalculatorJob, new Path(miInforExtractorIntermediatePath));
		FileOutputFormat.setOutputPath(miCalculatorJob, new Path(
				miCalculatorIntemediatePath));

		if (!isRunningInCloud) {
			Path outputDir0 = new Path(miCalculatorIntemediatePath);
			FileSystem.get(conf).delete(outputDir0, true);
		}
		startTime = System.currentTimeMillis();
		status = miCalculatorJob.waitForCompletion(true);
		L.log("miCalculatorJob Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		// Third job
		Job tripleJob = Job.getInstance(conf);
		tripleJob.setJobName("miCalculatorJob");
		tripleJob.setJarByClass(JobConstructor.class);
		tripleJob.setMapperClass(TripleDatabaseManufactorMapper.class);
		tripleJob.setReducerClass(TripleDatabaseManufactorReducer.class);
		
		// Set Output and Input Parameters
		tripleJob.setOutputKeyClass(Text.class);
		tripleJob.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(tripleJob, new Path(miCalculatorIntemediatePath));
		FileOutputFormat.setOutputPath(tripleJob, new Path(
				tripleDatabaseIntermediatePath));
		
		if (!isRunningInCloud) {
			Path outputDir0 = new Path(tripleDatabaseIntermediatePath);
			FileSystem.get(conf).delete(outputDir0, true);
		}
		startTime = System.currentTimeMillis();
		status = tripleJob.waitForCompletion(true);
		L.log("tripleJob Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
			System.exit(0);
	}	
}	
