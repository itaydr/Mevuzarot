package task1.work;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DeleteTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.AmazonServiceException;

public class LocalMachine {
	
	public static PropertiesCredentials Credentials;
	public static AmazonS3 S3;
	public static String bucketName = "mevuzarot.task1";
	// @itay: this file is not committed because github is public. Make sure you copy it before testing.
	public static String propertiesFilePath = "src/task1/work/_itay_creds.properties";
	
	private static AmazonEC2 ec2;
	private static Instance remoteManager;
	
	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException{
			
			LocalMachine.initS3();
			
			String fileToUploadPath = args[0];
			String pathInS3 = LocalMachine.uploadFileToS3(fileToUploadPath);
			
			QueueManager.sharedInstance().init(Credentials);
			QueueManager.sharedInstance().startJobWithFile(pathInS3);
			
			LocalMachine.startUpRemoteManager();
			
			LocalMachine.loopForMessages();
	}
	
	private static void loopForMessages () {
		List<Message> messages;
		
		while (true) {
			messages = QueueManager.sharedInstance().waitForMessages();
			for (Message msg : messages) {
				if (msg.getBody().equals("terminate")) {
					LocalMachine.shutDownRemoteManager();
				}
			}
		}
	}
	
	private static String uploadFileToS3 (String fileToUploadPath) {
		File f = new File(fileToUploadPath);
		String pathInS3 = f.getName();
		PutObjectRequest por = new PutObjectRequest(bucketName,pathInS3,f);
		// Upload the file
		S3.putObject(por);
		System.out.println("File uploaded.");
		
		return pathInS3;
	}
	
	
	private static void initS3 () {
		try {
			Credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Credentials created.");
		S3 = new AmazonS3Client(Credentials);
		System.out.println("AmazonS3Client created.");
		// If the bucket doesn't exist - will create it.
		// Notice - this will create it in the default region : Region.US_Standard
		if (!S3.doesBucketExist(bucketName)) {
			S3.createBucket(bucketName);
			System.out.println("Created bucket.");
		}
		else {
			System.out.println("Bucket exist.");
		}
	}
	
	private static boolean startUpRemoteManager () throws FileNotFoundException, IOException {
		boolean manager_exists = false;
		
		AWSCredentials credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
		ec2 = new AmazonEC2Client(credentials);

		DescribeInstancesResult result = ec2.describeInstances();
		List<Reservation> reservations = result.getReservations();

		for (Reservation reservation : reservations) {
			List<Instance> instances = reservation.getInstances();
			
			for (Instance instance : instances) {
				
				if ( instance.getTags().contains(new Tag("Manager", "True")) ) {
					manager_exists = true;
		    	  }	
		      }	
		 }	
		
		if (!manager_exists) {
			try {
				// Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
				RunInstancesRequest request = new RunInstancesRequest("ami-08728661", 1, 1);
				request.setInstanceType(InstanceType.T1Micro.toString());
				List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
				System.out.println("Launch instances: " + instances);
				
				remoteManager =  instances.get(0);
				
				//System.out.println(instances.get(0).getInstanceId());
				CreateTagsRequest createTagsRequest=new CreateTagsRequest().withResources(instances.get(0).getInstanceId()).withTags(new Tag("Manager","True"));
				ec2.createTags(createTagsRequest);
				
				return true;
				
			} catch (AmazonServiceException ase) {
				System.out.println("Caught Exception: " + ase.getMessage());
				System.out.println("Reponse Status Code: " + ase.getStatusCode());
				System.out.println("Error Code: " + ase.getErrorCode());
				System.out.println("Request ID: " + ase.getRequestId());
			}
		}
		
		return false;
	}
	
	private static boolean shutDownRemoteManager () {
		
		if (remoteManager != null) {
			DeleteTagsRequest createTagsRequest=new DeleteTagsRequest().withResources(remoteManager.getInstanceId()).withTags(new Tag("Manager","True"));
			ec2.deleteTags(createTagsRequest);
			System.out.println("tag removed for Manager");
			
			return true;
		}
	
		return false;
	}
}
