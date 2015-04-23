package task1;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;

import javax.imageio.IIOException;
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

	private static WorkerStats stats;
	
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
		
		stats = new WorkerStats();
		stats.uid = nodeid;
		
		inboundQueueFromManager = new QueueUtil(Credentials, TO_WORKERS_QUEUE_IDENTIFIER, nodeid);
		outboundQueueToManager =  new QueueUtil(Credentials, TO_MANAGER_QUEUE_IDENTIFIER, nodeid);
		s3_client = new S3Util(Credentials, bucketName);
		
		double totalComutationTime = 0;
		Date urlStart;
		while (true) {
			Message message = inboundQueueFromManager.getSingleBroadcastMessage(WORKER_TIMEOUT);
			
			if (null == message && stats.proccessedUrlsCount > 0) {
				stats.endDate = new Date();
				stats.averageUrlProcessTime = totalComutationTime / stats.proccessedUrlsCount;
				uploadStats();
				
				continue;
			}
			
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
				
				stats.endDate = new Date();
				stats.averageUrlProcessTime = totalComutationTime / stats.proccessedUrlsCount;
				uploadStats();
				
				inboundQueueFromManager.deleteMessageFromQueue(message);
				System.out.println("Got termination signal... quitting");
				return;
			}
			
			urlStart = new Date();
			stats.proccessedUrlsCount++;
			
			// work on url
			String urlPath = message.getBody();
			BufferedImage originalImage = null;
			try {
                System.out.println("working on: " + urlPath);
                
                originalImage = readImageFromUrl(urlPath);
                System.out.println("successfully downloaded");
                
            }
            catch (IIOException e) {
                System.out.println("Failed to download, removing message for file: " + urlPath);
                addFailedUrl(urlPath, e);
                inboundQueueFromManager.deleteMessageFromQueue(message);
                outboundQueueToManager.sendErrorToManager(urlPath);
                continue;
            }
            catch (Exception e) {
                System.out.println("Failed to download, not FileNotFound!: " + e);
                addFailedUrl(urlPath, e);
                continue;
            }
            
            try {
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
            } catch (Exception e) {
                System.out.println("Failed to crop or upload image - Not removing message. " + e);
                addFailedUrl(urlPath, e);
            }
            
            double duration = (new Date().getTime()) - urlStart.getTime();
            totalComutationTime += duration;
            
            stats.successfullUrls.add(urlPath);
		}
	}
	
	private static void addFailedUrl(String url, Exception e) {
		stats.failedUrls.add(new Failure(url, e.getMessage()));
	}
	
	private static void uploadStats() {
		try {
			String path = "WorkerStats_" + stats.uid + ".txt";
			PrintWriter out = new PrintWriter(path);
			out.print(stats.toString());
			out.close();
			
			s3_client.uploadFileToS3(path);
			
			removeFile(path);
			
		} catch (Exception e) {
			e.printStackTrace();
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

class WorkerStats {
	public String uid;
	public Date startDate;
	public Date endDate;
	public double averageUrlProcessTime;
	public int proccessedUrlsCount;
	public ArrayList<String> successfullUrls;
	public ArrayList<Failure> failedUrls;
	
	public WorkerStats() {
		super();
		
		this.successfullUrls = new ArrayList<String>();
		this.failedUrls = new ArrayList<Failure>();
		this.startDate = new Date();
	}
	
	public String toString() {
		
		String str = 	"Worker {" + uid + "}\n" +
						"Start date  = " + startDate +"\n" +
						"End date = " + endDate + "\n" +
						"Average proccess time in MS= " + averageUrlProcessTime + "\n" +
						"Number of processed url = " + proccessedUrlsCount + "\n";
		str += "Successfull urls - \n";
		for (int i = 0 ; i < successfullUrls.size() ; i++) {
			String url = successfullUrls.get(i);
			str += i + ": " +  url + "\n";
		}
		
		str += "\nFailed urls - \n";
		for (int i = 0 ; i < failedUrls.size() ; i++) {
			Failure fail = failedUrls.get(i);
			str += i + ": " + fail.url +", Error reason = " + fail.error + "\n";
		}
		
		
		return str;
	}
}

class Failure {
	
	public Failure (String url, String error) {
		super();
		
		this.url = url;
		this.error = error;
	}
	
	public String url;
	public String error;
}
