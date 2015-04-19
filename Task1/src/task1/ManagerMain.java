package task1;


import java.io.File;
import java.io.IOException;
import task1.work.QueueManager;

public class ManagerMain {


	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args){
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
	    
	    new RemoteManager().init();
	}


}
