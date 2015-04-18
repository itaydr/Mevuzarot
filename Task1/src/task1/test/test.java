package task1.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
public class test {
	
	public static PropertiesCredentials Credentials;
	public static AmazonS3 S3;
	public static String bucketName = "mevuzarot.task1";
	// @itay: this file is not committed because github is public. Make sure you copy it before testing.
	public static String propertiesFilePath = "src/task1/test/_itay_creds.properties";
	public static String fileToUploadPath = "src/task1/test/test.java";
	
	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException{
			Credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
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
			
			File f = new File(fileToUploadPath);
			PutObjectRequest por = new PutObjectRequest(bucketName,f.getName(),f);
			// Upload the file
			S3.putObject(por);
			System.out.println("File uploaded.");
	}
}