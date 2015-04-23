package task1;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;

import javax.imageio.ImageIO;

import org.imgscalr.Scalr;
import org.imgscalr.Scalr.Method;
import org.imgscalr.Scalr.Mode;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.model.Message;


public class Worker {
	//private static String nodeID;
	
	private static S3Util s3_client;
	private static String bucketName = "mevuzarot.task1";
	
	private static int WORKER_TIMEOUT = 60;
	
	//private final static String TO_LOCAL_QUEUE_IDENTIFIER 	= "mevuzarot_task1_to_local";
	private final static String TO_MANAGER_QUEUE_IDENTIFIER 	= "mevuzarot_task1_to_manager_2";
	private final static String TO_WORKERS_QUEUE_IDENTIFIER   = "mevuzarot_task1_to_workers_2";
	
	
	private static QueueUtil inboundQueueFromManager;
	private static QueueUtil outboundQueueToManager;
	
	private static PropertiesCredentials Credentials;
	private final static String propertiesFilePath = "src/task1/_itay_creds.properties";

	public static void main(String[] args) {
		// open credentials file
		try {
			Credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
		} catch (FileNotFoundException e) {
			System.out.println("Failed to open credentials file.");
			return;
		} catch (IOException e) {
			System.out.println("Failed to open credentials file.");
			return;
		}
		
		String response = null;
		try {
			response = new GetRequest("http://169.254.169.254/latest/meta-data/instance-id").execute();
		} catch (Exception e) {
			System.out.println(e.toString());
		}
		
		System.out.println("Worker id is - " + response);
		String nodeid = response;
		
		inboundQueueFromManager = new QueueUtil(Credentials, TO_WORKERS_QUEUE_IDENTIFIER, nodeid);
		outboundQueueToManager =  new QueueUtil(Credentials, TO_MANAGER_QUEUE_IDENTIFIER, nodeid);
		s3_client = new S3Util(Credentials, bucketName);
		
		
		while (true) {
			Message message = inboundQueueFromManager.getSingleBroadcastMessage(WORKER_TIMEOUT);
			if (null == message) {
				try {
					System.out.print(".");
					Thread.sleep(1 * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}
			System.out.println("");
			ArrayList<Message> temp = new ArrayList<Message>();
			temp.add(message);
			QueueUtil.debugMessagesForMe(temp);
			

			// check termination signal
			String type = message.getMessageAttributes().get("type").getStringValue();
			if ( type.equals(QueueUtil.MSG_TERMINATE) ) {
				inboundQueueFromManager.deleteMessageFromQueue(message);
				System.out.println("Got termination signal... quitting");
				return;
			}
			
			// work on url
			String urlPath = message.getBody();
			try {
				System.out.println("working on: " + urlPath);
				
				BufferedImage originalImage = readImageFromUrl(urlPath);
				System.out.println("successfully downloaded");
			
				BufferedImage thumbnailImage = resizeImage(originalImage, 50, 50);
				System.out.println("successfully resized");
						
				String thumbnailFileBasename = urlPath.substring(urlPath.lastIndexOf('/')+1);
				String thumbnailFilePath = "/tmp/" + thumbnailFileBasename;
				String outputFormat = urlPath.substring(urlPath.lastIndexOf('.')+1);
						
				saveThumbnailToFile(thumbnailFilePath, thumbnailImage, outputFormat);
				System.out.println("successfully saved to file");
			
				s3_client.uploadFileToS3(thumbnailFilePath);
				outboundQueueToManager.finishWorkNotify(urlPath , thumbnailFileBasename);
			
				inboundQueueFromManager.deleteMessageFromQueue(message);
				removeFile(thumbnailFilePath);
			}
			catch (Exception e) {
				System.out.println("Failed to download: " + urlPath);
				inboundQueueFromManager.deleteMessageFromQueue(message);
				outboundQueueToManager.sendErrorToManager(urlPath);
			}
		}
	}

	private static void removeFile(String thumbnailFilePath) {
		
		File file = new File(thumbnailFilePath);
		try {
		    if ( file.delete()) {
		    	System.out.println("file deleted: "+ thumbnailFilePath);
		    } else {
		    	System.out.println("Failed to delete File: "+ thumbnailFilePath);
		    }
		} catch (Exception x) {
			System.out.println("Failed to delete File: "+ thumbnailFilePath);
		}
	}

	private static void saveThumbnailToFile(String thumbnailFilePath, 
			BufferedImage thumbnailImage,
			String outputFormat) {
		  
		File f2 = new File(thumbnailFilePath);
		
		try {
			ImageIO.write(thumbnailImage, outputFormat, f2);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Failed to write output path");
		}
	}

	private static BufferedImage resizeImage(BufferedImage originalImage, int width, int height) {
		
		System.out.println("current size: " + originalImage.getHeight(null) + " " + originalImage.getWidth(null));
		
		
		BufferedImage thumbImg = Scalr.resize(
				originalImage, 
				Method.QUALITY,
				Mode.AUTOMATIC, 
				50, 
				50, 
				Scalr.OP_ANTIALIAS);
		
		return thumbImg;
	}
	
	private static BufferedImage readImageFromUrl(String urlPath) throws IOException {
		BufferedImage image = null;
		
	    URL url = new URL(urlPath);
	    image = ImageIO.read(url);
		
		return image;
	}
}
