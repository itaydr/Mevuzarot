package task1;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

import javax.imageio.IIOException;
import javax.imageio.ImageIO;
import org.imgscalr.Scalr;
import org.imgscalr.Scalr.Method;
import org.imgscalr.Scalr.Mode;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.model.Message;


public class Worker {
	private static S3Util s3_client;
	
	private static QueueUtil inboundQueueFromManager;
	private static QueueUtil outboundQueueToManager;
	
	private static PropertiesCredentials Credentials;

	private static WorkerStats stats;
	
	private static Integer imgCounter;
	private static PrintWriter out; 
	
	public static void main(String[] args) throws IOException {
		File file = new File (Config.workerLogFilePath);
		out = new PrintWriter(file);
		
		// open credentials file
		try {
			Credentials = new PropertiesCredentials(new FileInputStream(Config.propertiesFilePath));
		} catch (FileNotFoundException e) {
			System.out.println("Failed to open credentials file: " + Config.propertiesFilePath);
			return;
		} catch (IOException e) {
			System.out.println("Failed to open credentials file: " + Config.propertiesFilePath);
			return;
		}
		
		imgCounter = 0;

		String response = null;
		try {
			response = new GetRequest("http://169.254.169.254/latest/meta-data/instance-id").execute();
		} catch (Exception e) {
			System.out.println(e.toString());
			response = "WORKER-" + UUID.randomUUID().toString();
		}
		
		System.out.println("Worker id is - " + response);
		String nodeid = response;
		
		stats = new WorkerStats();
		stats.uid = nodeid;
		
		inboundQueueFromManager = new QueueUtil(Credentials, Config.TO_WORKERS_QUEUE_IDENTIFIER, nodeid);
		outboundQueueToManager =  new QueueUtil(Credentials, Config.TO_MANAGER_QUEUE_IDENTIFIER, nodeid);
		s3_client = new S3Util(Credentials, Config.bucketName);
	
		double totalComutationTime = 0;
		Date urlStart;
		String thumbnailFilePath = null;
		try {
			while (true) {
				System.out.println("trying to get BroadcastMessage.");
				Message message = inboundQueueFromManager.getSingleBroadcastMessage(Config.WORKER_TIMEOUT);
				if (null == message) {
					try {
						System.out.print(".");
						Thread.sleep(1000 * Config.randomSleep(1,3));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					continue;
				}
				System.out.println("found not null message");
				System.out.println("");
				ArrayList<Message> temp = new ArrayList<Message>();
				temp.add(message);
				QueueUtil.debugMessagesForMe(temp);
				
				// check termination signal
				String type = message.getMessageAttributes().get("type").getStringValue();
				if ( type.equals(QueueUtil.MSG_TERMINATE) ) {
					System.out.println("This is Terminate message");
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
				String thumbnailFileBasename  = null;
//				BufferedImage originalImage = null;
				BufferedImage thumbnailImage;
				try {
	                System.out.println("working on url: " + urlPath);
	                
	                thumbnailImage = downloadAndThumb(urlPath, 50, 50);
	                
	                thumbnailFileBasename = nodeid + "_" + imgCounter.toString();
	            	imgCounter++;
	            	
	            	String outputFormat = urlPath.substring(urlPath.lastIndexOf('.')+1);
	            	
	                thumbnailFilePath = "/tmp/" + thumbnailFileBasename;
	                if (false == saveThumbnailToFile(thumbnailFilePath, thumbnailImage, outputFormat)) {
	                	throw new Exception("Failed to save file with format type: " + outputFormat);
	                }
	                
	                System.out.println("successfully saved to file: " + thumbnailFilePath);
	            }
	            catch (IIOException e) {
	                System.out.println("Failed to download, removing message for file: " + urlPath);
	                debugLog("error4 - " + urlPath);
	                addFailedUrl(urlPath, e);
	                inboundQueueFromManager.deleteMessageFromQueue(message);
	                outboundQueueToManager.sendErrorToManager(urlPath);
	                continue;
	            }
				catch (NullPointerException e) {
	                System.out.println("Failed to resize - content not an image(?), removing message for file: " + urlPath);
	                debugLog("error3 - " + urlPath);
	                addFailedUrl(urlPath, e);
	                inboundQueueFromManager.deleteMessageFromQueue(message);
	                outboundQueueToManager.sendErrorToManager(urlPath);
	                continue;
				}
	            catch (Exception e) {
	                System.out.println("Failed to download, general exception, not FileNotFound!: " + e);
	                debugLog("error2 - " + urlPath);
	                addFailedUrl(urlPath, e);
	                inboundQueueFromManager.deleteMessageFromQueue(message);
	                outboundQueueToManager.sendErrorToManager(urlPath);
	                continue;
	            }
	            
	            try {
	                s3_client.uploadFileToS3(thumbnailFilePath);
	                System.out.println("successfully uploaded to S3");
	                
	                outboundQueueToManager.finishWorkNotify(urlPath , thumbnailFileBasename);
	                System.out.println("sending finish work");
	                debugLog("ok - " + urlPath);
	                
	                inboundQueueFromManager.deleteMessageFromQueue(message);
	                System.out.println("deleting from queue");
	                
	                removeFile(thumbnailFilePath);
	                System.out.println("local file deleted");
	            } catch (Exception e) {
	                System.out.println("general failiure - Not removing message. saveToLocalFile / uploadToS3 / Queue..." + e);
	                debugLog("error1 - " + urlPath);
	                addFailedUrl(urlPath, e);
	                continue;
	            }
	            
	            double duration = (new Date().getTime()) - urlStart.getTime();
	            totalComutationTime += duration;
	            
	            stats.successfullUrls.add(urlPath);
	            System.out.println("going to another round..." + (new Date().getTime()));
			}
		} catch (Exception e) {
			System.out.println("There is an unhandeled exception! " + e);
		}
		out.close();
	}
	
	private static BufferedImage downloadAndThumb(String urlPath, int h, int w) throws Exception {

		BufferedImage originalImage = null;
		BufferedImage thumbnailImage = null;
		
		
        try {
			originalImage = readImageFromUrlMethod1(urlPath);
			System.out.println("downloaded method1");
			thumbnailImage = resizeImage(originalImage, 50, 50);
			System.out.println("successfully resized");
		} catch (Exception e) {
			System.out.println("got Exception in method 1... trying method 2");
			e.printStackTrace();
			
			originalImage = readImageFromUrlMethod2(urlPath);
			System.out.println("downloaded method2");
	        thumbnailImage = resizeImage(originalImage, 50, 50);
	        System.out.println("successfully image has been resized");
		}
        
		return thumbnailImage;
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

	private static boolean saveThumbnailToFile(String thumbnailFilePath, 
			BufferedImage thumbnailImage,
			String outputFormat) {
		  
		File f2 = new File(thumbnailFilePath);
		System.out.println("The file fotmat is: " + outputFormat);
		try {
			if (outputFormat.toLowerCase().equals("jpe")) {
				System.out.println(" [+] handling jpe file.. as jpg");
				return ImageIO.write(thumbnailImage, "jpg", f2);
			} else {
				return ImageIO.write(thumbnailImage, outputFormat, f2);
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Failed to write output path");
		}
		return false;
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
	
	private static BufferedImage readImageFromUrlMethod1(String urlPath) throws Exception {
		BufferedImage image = null;
		
	    URL url = new URL(urlPath);
    	// try method 1
    	image = ImageIO.read(url);
	    
		return image;
	}
	
	private static BufferedImage readImageFromUrlMethod2(String urlPath) throws Exception {
		BufferedImage image = null;
		
	    URL url = new URL(urlPath);
    	// try method 2
	    HttpURLConnection.setFollowRedirects(true); 
    	HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    	conn.setInstanceFollowRedirects(true);
    	conn.setReadTimeout(5000);
    	conn.addRequestProperty("Accept-Language", "en-US,en;q=0.8");
    	conn.addRequestProperty("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
    	
    	// normally, 3xx is redirect
    	int status = conn.getResponseCode();
    	System.out.println("got: " + status);
    	System.out.println("headers: " + conn.getHeaderFields().toString());
    	while (status != HttpURLConnection.HTTP_OK 
    		&& (status == HttpURLConnection.HTTP_MOVED_TEMP
    			|| status == HttpURLConnection.HTTP_MOVED_PERM
    				|| status == HttpURLConnection.HTTP_SEE_OTHER)){
    		
	    	// get redirect url from "location" header field
			String newUrl = conn.getHeaderField("Location");
	 
			// get the cookie if need, for login
			String cookies = conn.getHeaderField("Set-Cookie");
	 
			// open the new connnection again
			conn = (HttpURLConnection) new URL(newUrl).openConnection();
			conn.setRequestProperty("Cookie", cookies);
			conn.addRequestProperty("Accept-Language", "en-US,en;q=0.8");
			conn.addRequestProperty("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
	 
			System.out.println("Redirect to URL : " + newUrl);
			status = conn.getResponseCode();
    	}
    	
    	image = ImageIO.read(conn.getInputStream());
		return image;
	}
	
	private static void debugLog(String line) {
		out.println(line);
		out.flush();
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
