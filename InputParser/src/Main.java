import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;


public class Main {
	
	
	
	public static void main(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.out.println("2 args min");
			return;
		}
		
		String inputPath = args[0];
		String outPath = args[1];
		String[] parts = null;
		
		PrintWriter writer = new PrintWriter(outPath, "UTF-8");
		BufferedReader br = new BufferedReader(new FileReader(inputPath));
		try {
		    String line = br.readLine();
		    while (line != null) {
		    	parts = line.trim().split("\t");
		    	if (parts.length == 2) {	
			    	writer.println("\""+parts[0] + "\" + Constants.S + \"" + parts[1]+ "\",");
			        line = br.readLine();
		    	}
		    }
		} finally {
			writer.close();
		    br.close();
		    System.out.println("Done");
		}
	}
}
