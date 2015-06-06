package task1;

import java.io.FileInputStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class AWSManager {

	private static final String ENG_2GRAMS_PATH = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-1M/2gram/data";
	//private static final String ENG_2GRAMS_PATH = "s3n://mevuzarot.task2/eng-2gram-1m";;
	private static final String HEB_2GRAMS_PATH = "s3n://mevuzarot.task2/heb-2gram-10K";//"";
	private static String ENG_BIGRAM_COUNT = "6626604215";
	private static String HEB_BIGRAM_COUNT = "252069581";
	private static final String OUTPUT_PATH  = "s3n://mevuzarot.task2/output/";
	
	public static void main(String[] args) throws Exception {
		
		if (args.length < 4) {
			System.out.println("Not enough aruments to run the job.");
			System.exit(0);
		}
		
		String minPmi = args[0];
		String relMinPmi = args[1];
	    boolean useEnglishFile = args[2].equals("eng");
	    String useStopWords = args[3];
	    
	    AWSCredentials credentials = new PropertiesCredentials(new FileInputStream(Credentials.propertiesFilePath));
	    AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
	     
	    HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
	        .withJar("s3n://mevuzarot.task2/ass2.jar") // This should be a full map reduce application.
	        .withArgs(useEnglishFile ? ENG_2GRAMS_PATH : HEB_2GRAMS_PATH,
	        		OUTPUT_PATH,
	        		minPmi,
	        		relMinPmi,
	        		useStopWords,
	        		useEnglishFile ? ENG_BIGRAM_COUNT : HEB_BIGRAM_COUNT,
	        		"1",
	        		useEnglishFile ? "eng" : "heb"
	        		);
	     
	    StepConfig stepConfig = new StepConfig()
	        .withName("stepname")
	        .withHadoopJarStep(hadoopJarStep)
	        .withActionOnFailure("TERMINATE_JOB_FLOW");
	     
	    JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
	        .withInstanceCount(6)
	        .withMasterInstanceType(InstanceType.M1Medium.toString())
	        .withSlaveInstanceType(InstanceType.M1Medium.toString())
	        .withHadoopVersion("2.2.0")
	        //.withEc2KeyName(Credentials.KEY_PAIR)
	        .withKeepJobFlowAliveWhenNoSteps(false)
	        //.withPlacement(new PlacementType("us-east-1a"))
	        ;
	     
	    RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
	        .withName("jobname")
	        .withInstances(instances)
	        .withSteps(stepConfig)
	        .withLogUri("s3n://mevuzarot.task2/logs/")
	        .withServiceRole("EMR_DefaultRole")
	        .withJobFlowRole("EMR_EC2_DefaultRole")
	        ;
	     
	    RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
	    String jobFlowId = runJobFlowResult.getJobFlowId();
	    System.out.println("Ran job flow with id: " + jobFlowId);
	}
}
