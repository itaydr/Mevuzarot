package task1;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;

import javax.imageio.ImageIO;


public class TESTManagerMainTEST {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException{
		
		ArrayList<String> output = new ArrayList<String>();
		output.add("hello");
		output.add("world");
		output.add("blaa");

		String outputPath = "/tmp/1234";
		File file = new File (outputPath);
		PrintWriter out;
		
		out = new PrintWriter(file);
		for (String line : output) {
			out.println(line);
		}
		out.close();
		
		
		/*
		String urlPath = "http://2.bp.blogspot.com/-0E-ZGvOdsCY/URei9x5buRI/AAAAAAAATFM/ouvJZpwVYG4/s1600/Alexander%2BMcQueen%2Bskull%2Bheels%2Binspired%2BDIY%2B-%2Bblack%2Bpeep%2Btoe%2Bpumps%2B.jpg";
		//String safeurlPath = URLEncoder.encode(urlPath);
		BufferedImage image = null;
		
		
		
		System.out.println(urlPath);
	    URL url = new URL(urlPath);
	    
	    URLConnection connection = url.openConnection();
	    //connection.setRequestProperty("User-Agent", "xxxxxx");
	    System.out.println(url);
	    //BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
	    image = ImageIO.read(url);
	    
		*/
		return ;
		/*
		HashMap h = new HashMap();
		h.put("a", true);
		
		System.out.println(h.get("a"));
		System.out.println(h.size());
		
		h.put("a", false);
		System.out.println(h.get("a"));
		System.out.println(h.size());
		
		System.out.println(h.containsKey("a"));
		*/
		
		/*
		ProcessBuilder builder = new ProcessBuilder("echo", "1234");
		builder.redirectOutput(new File("/tmp/a"));
		builder.redirectError(new File("/tmp/a"));
		try {
			Process p = builder.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // throws IOException
	    
	    System.out.println("done");
	    */
	}

}
