package task1;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
//import java.util.Date;

import javax.imageio.IIOException;
import javax.imageio.ImageIO;
import org.imgscalr.Scalr;
import org.imgscalr.Scalr.Method;
import org.imgscalr.Scalr.Mode;

//import com.amazonaws.auth.PropertiesCredentials;


public class worker_test {
/*	private static S3Util s3_client;
	
	private static QueueUtil inboundQueueFromManager;
	private static QueueUtil outboundQueueToManager;
	
	private static PropertiesCredentials Credentials;*/

	private static WorkerStats stats;
	
	private static Integer imgCounter;
	
	public static void main(String[] args) throws IOException {
		String nodeid = "aaa";
		
//		double totalComutationTime = 0;
//		Date urlStart;
		
//		urlStart = new Date();

		
		// work on url
//			String urlPath = "http://eoimages.gsfc.nasa.gov/images/imagerecords/6000/6415/glenda_tmo_2006088_lrg.jpg";
//			http://eoimages.gsfc.nasa.gov/images/imagerecords/16000/16168/carina_AMO_2006058_lrg.jpg
//			String urlPath = "http://www.clickbd.com/global/classified/item_img/814309_0_original.jpg";
//			String urlPath = "http://static.clickbd.com/global/classified/item_img/814309_0_original.jpg";
//			String urlPath = "http://www.martialartsactionmovies.com/wp-content/uploads/2013/03/Leonidas-Leads.jpg";
		
//			String urlPath = "https://archialternative.files.wordpress.com/2011/02/house-of-cards.jpg";
		String urlPath = args[0];
		
		
		
		

//		BufferedImage originalImage = null;
		BufferedImage thumbnailImage;
		try {
            System.out.println("working on: " + urlPath);
            
//            originalImage = readImageFromUrl(urlPath);
//            System.out.println("successfully downloaded web page");
            
//            thumbnailImage = resizeImage(originalImage, 50, 50);
//            System.out.println("successfully resized");
            
//            originalImage = readImageFromUrl(urlPath);
//            System.out.println("successfully downloaded web page");
            
            thumbnailImage = readImageFromUrl(urlPath);
            System.out.println("successfully resized");
            
            
        }
        catch (IIOException e) {
        	System.out.println("e " + e);
            System.out.println("Failed to download, removing message for file: " + urlPath);
            addFailedUrl(urlPath, e);
//                inboundQueueFromManager.deleteMessageFromQueue(message);
//            outboundQueueToManager.sendErrorToManager(urlPath);
            return;
        }
		catch (NullPointerException e) {
			System.out.println("e " + e);
            System.out.println("Failed to resize - content not an image(?), removing message for file: " + urlPath);
            addFailedUrl(urlPath, e);
//                inboundQueueFromManager.deleteMessageFromQueue(message);
//            outboundQueueToManager.sendErrorToManager(urlPath);
            return;
		}
        catch (Exception e) {
        	System.out.println("e " + e);
            System.out.println("Failed to download, not FileNotFound!: " + e);
            addFailedUrl(urlPath, e);
            return;
        }
        
        try {
        	imgCounter = 1;
        	String thumbnailFileBasename = nodeid + "_" + imgCounter.toString();
        	imgCounter++;
        	
        	String outputFormat = urlPath.substring(urlPath.lastIndexOf('.')+1);
        	
            String thumbnailFilePath = "/tmp/" + thumbnailFileBasename;
            if (false == saveThumbnailToFile(thumbnailFilePath, thumbnailImage, outputFormat)) {
            	throw new Exception("Failed to save file with format type: " + outputFormat);
            }
            
            System.out.println("successfully saved to file: " + thumbnailFilePath);
            
        } catch (Exception e) {
            System.out.println("Failed to crop or upload image - Not removing message. " + e);
            addFailedUrl(urlPath, e);
            return;
        }
            
	}
	
	private static void addFailedUrl(String url, Exception e) {
		stats.failedUrls.add(new Failure(url, e.getMessage()));
	}
	/*
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
	}*/

	private static boolean saveThumbnailToFile(String thumbnailFilePath, 
			BufferedImage thumbnailImage,
			String outputFormat) {
		  
		File f2 = new File(thumbnailFilePath);
		
		try {
			if (outputFormat.toLowerCase().equals("jpe")) {
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
	
	private static BufferedImage readImageFromUrl(String urlPath) throws IOException {
		BufferedImage image = null;
		
	    URL url = new URL(urlPath);
	    try {
	    	System.out.println("HHHHHHHHHHHHHHHHHHHHHHHHH");
	    	image = ImageIO.read(url);
	    	System.out.println("TTTTTTTTTTTTTTTTTTTTTTTTT");
	    	image = resizeImage(image, 50, 50);
	    	System.out.println("QQQQQQQQQQQQQQQQQQQQQQQQQ");
	    } catch (Exception e) {
	    	System.out.println("EEEEEEEEEEEEEEEEEEEEEEEEE");
	    	System.out.println("e " + e);
	    	
	    	
	    	
	    	HttpURLConnection.setFollowRedirects(true); 
	    	HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	    	conn.setInstanceFollowRedirects(true);
	    	conn.setReadTimeout(5000);
	    	conn.addRequestProperty("Accept-Language", "en-US,en;q=0.8");
	    	conn.addRequestProperty("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
	    	
//	    	boolean redirect = false;
	    	 
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
	    	
	    	/*URLConnection uc = url.openConnection();
	        uc.addRequestProperty("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");

	        uc.connect();
	    	
	    	*/
	    	image = ImageIO.read(conn.getInputStream());
	    	System.out.println("DDDDDDDDDDDDDDDDDDDDDDDDD");
	    	image = resizeImage(image, 50, 50);
	    	System.out.println("CCCCCCCCCCCCCCCCCCCCCCCCC");
		}
		
		return image;
	}
}
