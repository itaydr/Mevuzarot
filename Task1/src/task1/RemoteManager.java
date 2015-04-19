package task1;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.List;
import java.util.ArrayList;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import task1.work.LocalMachine;
import task1.work.QueueManager;

public class RemoteManager {

	private QueueManager queue;
	private static String QUEUE_IDENTIFIER = "mevuzarot_task1_remotequeue";
	private boolean mTerminated;
	
	public static PropertiesCredentials Credentials;
	public static AmazonS3 S3;
	public static String bucketName = "mevuzarot.task1";
	public static String propertiesFilePath = "?";
	
	public RemoteManager() {
		super();
	}
	
	public void init() {
		// @itay: We need to think of a solution for the properties file - or just upload it to S3 and put the path in propertiesFilePath 
		initS3();
		this.startListening();
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
	private void startListening() {
		this.queue = new QueueManager(QUEUE_IDENTIFIER);
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				List<Message> messages;
				
				while (true) {
					messages = queue.waitForMessages();
					if (messages != null) {
						for (Message msg : messages) {
							handleMessage(msg);
						}
					}
					try {
						// Temp - queue seems to not be blocking.
						Thread.sleep(100);
					} catch (InterruptedException e) {}
				}
				
				
			}
		}).start();
	}
	
	private void handleMessage(Message msg) {
		MessageAttributeValue value = msg.getMessageAttributes().get("type");
		if (value == null)
			return;
		
		String type = value.getStringValue();
		if (type.equals(QueueManager.TERMINATE_TYPE_KEY)) {
			mTerminated = true;
		}
		else if (type.equals(QueueManager.START_JOB_TYPE_KEY)) {
			String path = msg.getBody();
			
			if (mTerminated) {
				System.out.println("Cannot start new job - terminated");
			}
			else {
				List<String> files =  downloadFileFromS3(path);
				for (String file : files) {
					queue.sendMessage(file, QueueManager.REMOTE_MANAGER_KEY, null, QueueManager.START_JOB_TYPE_KEY);
				}
			}
		}
		else if (type.equals(QueueManager.JOB_ENDED_TYPE_KEY)) {
			System.out.println("A worker ended parsing one file");
		}
	}
	
	private List<String> downloadFileFromS3 (String path) {
		
		System.out.println("File uploaded.");
		
		GetObjectRequest get = new GetObjectRequest(bucketName, path);
		S3Object object = S3.getObject(get);
		
		InputStream is = object.getObjectContent();
		
		// TODO
		// Get the lines from the Input Stream
		
		return new ArrayList<String>();
	}
}
