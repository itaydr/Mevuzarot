package task1;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class S3Util {
	private static String bucketName;
	private static PropertiesCredentials Credentials;
	private static AmazonS3 S3;
	
	public S3Util(PropertiesCredentials creds, String bucket) {
		bucketName = bucket;
		Credentials = creds;
		S3 = new AmazonS3Client(Credentials);
		if (!S3.doesBucketExist(bucketName)) {
			S3.createBucket(bucketName);
			System.out.println("Created bucket.");
		}
		else {
			System.out.println("Bucket exist.");
		}
	}
	
	public String uploadFileToS3(String fileToUploadPath){
		File f = new File(fileToUploadPath);
		String pathInS3 = f.getName();
		
        AccessControlList acl = new AccessControlList();
        acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);
        PutObjectRequest por = new PutObjectRequest(bucketName,pathInS3,f).withAccessControlList(acl);
        
        
		// Upload the file
		S3.putObject(por);
		System.out.println("File " + fileToUploadPath + " uploaded To: " + pathInS3);
		
		return pathInS3;
	}
	
	public ArrayList<String> getFileContentFromS3(String path){
		GetObjectRequest get = new GetObjectRequest(bucketName, path);
		S3Object object = S3.getObject(get);
		
		ArrayList<String> output = new ArrayList<String>(); 
		
		InputStream is = object.getObjectContent();
		
	    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        while (true) {
            String line;
			try {
				line = reader.readLine();
			} catch (IOException e) {
				System.out.println("Error reading from input stream");
				line = null;
			}
            if (line == null) break;
 
            output.add(line);
            //System.out.println("    " + line);
        }
    
        return output;
	}

}
