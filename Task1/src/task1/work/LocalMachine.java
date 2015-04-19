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

public class LocalMachine {
	
	public static PropertiesCredentials Credentials;
	public static AmazonS3 S3;
	public static String bucketName = "mevuzarot.task1";
	// @itay: this file is not committed because github is public. Make sure you copy it before testing.
	public static String propertiesFilePath = "src/task1/work/_itay_creds.properties";
	
	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException{
			
			LocalMachine.initS3();
			
			String fileToUploadPath = args[0];
			String pathInS3 = LocalMachine.uploadFileToS3(fileToUploadPath);
			
			QueueManager.sharedInstance().init(Credentials);
			QueueManager.sharedInstance().startJobWithFile(pathInS3);
			
			LocalMachine.loopForMessages();
	}
	
	private static void loopForMessages () {
		List<Message> messages;
		
		while (true) {
			messages = QueueManager.sharedInstance().waitForMessages();
			// Process messages.
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
}
