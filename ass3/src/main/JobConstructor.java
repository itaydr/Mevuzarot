package main;

import mapreduce.MIInfoExtractor.MIInfoExtractorMapper;
import mapreduce.MIInfoExtractor.MIInfoExtractorReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import Utils.Constants;
import Utils.DLogger;
import mapreduce.Partitioner;

public class JobConstructor {
		
	final static DLogger L = new DLogger(true);
	private final static int MIN_INPUT_ARGS_COUNT = 2;
	
	///*
	private static final String MIINFO_EXTRACTOR_INTERMEDIATE_PATH = "s3n://mevuzarot.task2/intermediate/mi_info_extractor";
	//*/
	/*
	private static final String MIINFO_EXTRACTOR_INTERMEDIATE_PATH = "/user/hduser/mi_info_extractor";
	*/
	
	/**
	 * Input - 
	 * 	0. Input path.
	 * 	1. Output path.
	 * 	2. DPMinCount
	 * 	3. MinFeatureNum 
	 * 	4. Is running in cloud
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

		L.log("JobCostructor start:");
		L.log(" - input path: " + inputPath);
		L.log(" - output path: " + outputPath);
		L.log(" - MIInfoExtractor path: " + miInforExtractorIntermediatePath);
		
		Configuration conf = new Configuration();

		conf.set("DPMinCount", args[2]);
		conf.set("MinFeatureNum", args[3]);
		
		boolean isRunningInCloud = args[4].equals("1");

		long startTime;
		boolean status;
		
		// Second job
		Job miInfoExtractorJob = Job.getInstance(conf);
		miInfoExtractorJob.setJobName("miInfoExtractorJob");
		miInfoExtractorJob.setJarByClass(JobConstructor.class);
		miInfoExtractorJob.setMapperClass(MIInfoExtractorMapper.class);
		miInfoExtractorJob.setPartitionerClass(mapreduce.Partitioner.DuplicateKeysPartitioner.class);
		miInfoExtractorJob.setReducerClass(MIInfoExtractorReducer.class);

		// Set Output and Input Parameters
		miInfoExtractorJob.setOutputKeyClass(Text.class);
		miInfoExtractorJob.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(miInfoExtractorJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(miInfoExtractorJob, new Path(miInforExtractorIntermediatePath));
		
		if (!isRunningInCloud) {
			Path outputDir0 = new Path(miInforExtractorIntermediatePath);
			FileSystem.get(conf).delete(outputDir0, true);
		}
		startTime = System.currentTimeMillis();
		status = miInfoExtractorJob.waitForCompletion(true);
		L.log("PairsPmiCounter Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		System.exit(0);	
	}
}