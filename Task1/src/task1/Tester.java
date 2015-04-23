package task1;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
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

public class Tester {

	/**
	 * @param args
	 */
	private static PropertiesCredentials Credentials;
	private static AmazonS3 S3;
	
	// @itay: this file is not committed because github is public. Make sure you copy it before testing.
	private final static String propertiesFilePath = "src/task1/_itay_creds.properties";
	private final static String bucketName = "mevuzarot.task1";
	
	
	private static AmazonEC2 ec2;
	private static Instance remoteManager;
	private static S3Util s3_client;
	
	private static QueueUtil outboundQueueToWorkers;
	
	private final static String TO_WORKERS_QUEUE_IDENTIFIER = "mevuzarot_task1_to_workers";
	
	public static void main(String[] args) {
		

		try {
			Credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
		} catch (FileNotFoundException e) {
			System.out.println("Failed to open credentials file.");
			return;
		} catch (IOException e) {
			System.out.println("Failed to open credentials file.");
			return;
		}
		
		outboundQueueToWorkers =  new QueueUtil(Credentials, TO_WORKERS_QUEUE_IDENTIFIER, null);
		
		
		
		
		for (int i = 0; i < 10; i++) {
			System.out.println(i);
			//outboundQueueToWorkers.sendSingleURLWork("http://25.media.tumblr.com/tumblr_m8lq6bxgpV1ra6ujbo1_1280.jpg");
			Message m = outboundQueueToWorkers.getSingleBroadcastMessage(10);
			if (m == null)
				break;
			System.out.println(m);
			System.out.println(m.getBody());
			if (i < 4)
				outboundQueueToWorkers.deleteMessageFromQueue(m);
			
		}
		System.out.println("queue:");
		System.out.println(outboundQueueToWorkers.queryNumberOfMessagesInQueue());
		System.out.println("done");
		
		
		//s3_client = new S3Util(Credentials, bucketName);
		
		//s3_client.uploadFileToS3("/home/asaf/Desktop/Mevuzarot/creds/test.txt");
		/*
		ArrayList<String> out;
		out = s3_client.getFileContentFromS3("in.txt");
		
		for (int i = 0; i < out.size(); i++){
			System.out.println(out.get(i));
		}
		System.out.println("done");
		
		*/
	

	}

}
