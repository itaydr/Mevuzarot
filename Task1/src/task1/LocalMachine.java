package task1;



import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.ec2.model.Instance;

public class LocalMachine {
	
	private static PropertiesCredentials Credentials;
	
	private static final String MANAGER_JAR_NAME = "Manager.jar";
	private static final String MANAGER_JAR_MAIN_CLASS = "task1.Manager";
	private static String MANAGER_JAR_PARAMETERS;
	
	// @itay: this file is not committed because github is public. Make sure you copy it before testing.
	private final static String propertiesFilePath = "src/task1/_itay_creds.properties";
	private final static String bucketName = "mevuzarot.task1";
	private final static String TO_LOCAL_QUEUE_IDENTIFIER 		= "mevuzarot_task1_to_local_2";
	private final static String TO_MANAGER_QUEUE_IDENTIFIER 	= "mevuzarot_task1_to_manager_2";
	//private final static String TO_WORKERS_QUEUE_IDENTIFIER   = "mevuzarot_task1_to_workers";
	
	
	//private static AmazonEC2 ec2;
	private static EC2Util ec2;
	private static Instance remoteManagerInstance;
	private static S3Util s3_client;
	private static QueueUtil inboundQueueFromManager;
	private static QueueUtil outboundQueueToManager;
	
	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException{
			if (args.length != 2 && args.length != 3) {
				System.out.println("Must provide arguments for input and output");
				return;
			}else {
				File f = new File(args[0]);
				if(! (f.exists() && !f.isDirectory())) {
					System.out.println("input file does not exists!");
					return;
				}
				MANAGER_JAR_PARAMETERS = args[1];
			}			
			// initialize credentials
			try {
				Credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
			} catch (FileNotFoundException e) {
				System.out.println("Failed to open credentials file.");
				return;
			} catch (IOException e) {
				System.out.println("Failed to open credentials file.");
				return;
			}

			// initialize queues, S3 and ec2_client
			inboundQueueFromManager = new QueueUtil(Credentials, TO_LOCAL_QUEUE_IDENTIFIER, "itay");
			outboundQueueToManager =  new QueueUtil(Credentials, TO_MANAGER_QUEUE_IDENTIFIER, "itay");
			s3_client = new S3Util(Credentials, bucketName);
			ec2 = new EC2Util(Credentials);
			
			// create or find manager instance
//			if (false == LocalMachine.startUpRemoteManager(args[1])) {
//				System.out.println("Failed to get Manager instance.. quitting");
//				return;
//			}
			
			// upload job file to S3
			String fileToUploadPath = args[0];
			System.out.println("Input - " + fileToUploadPath);
			String pathInS3 = s3_client.uploadFileToS3(fileToUploadPath);

			// send start job message to manager
			outboundQueueToManager.startJobWithFile(pathInS3);
			
			// wait for manager to finish
			Message receivedMessage = LocalMachine.loopForSingleMessage();
			if ( receivedMessage.getBody().equals(QueueUtil.MSG_TERMINATE) ) {
				System.out.println("Manager is terminated... the job has not been accepted");
				return;
			}
			String summaryPath = receivedMessage.getBody();
			ArrayList<String> thumbnailsUrls = s3_client.getFileContentFromS3(summaryPath);
			createHTMLFile(thumbnailsUrls, null);
			
			// send termination message
			if(args[2].equals("terminate")) {
				outboundQueueToManager.sendTerminationSignal();
				// wait for manager to terminate
				receivedMessage = LocalMachine.loopForSingleMessage();
				if (! ( receivedMessage.getBody().equals(QueueUtil.MSG_TERMINATE) )) {
					System.out.println("Manager did not send us termination ACK... ?!?");
					ArrayList<Message> tempList = new ArrayList<Message>();
					tempList.add(receivedMessage);
					QueueUtil.printMessages(tempList);
					return;
				}
				
				shutDownRemoteManager();
			}
			
	}
	
	private static void createHTMLFile(ArrayList<String> thumbnailsUrls,
			String outputFilePath) {
		String output = "<HTML>\n<HEAD>\n</HEAD>\n<BODY>\n";
		
		for (String line : thumbnailsUrls) {
			String origURL  = line.substring(0, line.indexOf(';'));
			String thumbURL  = "http://" + bucketName +".s3.amazonaws.com/" + line.substring(line.indexOf(';')+1);
			output += "<a href=\"" + origURL + "\"><img src=\"" + thumbURL + "\" width=\"50\" height=\"50\"></a>\n";
		}
		
		output += "</BODY>\n</HTML>";
		
		if ( null == outputFilePath ) {
			System.out.println(output);
		} else {
			try {
				File file = new File (outputFilePath);
				PrintWriter out = new PrintWriter(file);
				out.println(output);
				out.close();
			} catch (FileNotFoundException e) {
				System.out.println("Failed to write to file: "+ outputFilePath);
				System.out.println(output);
			}
		}
		
	}

	private static Message loopForSingleMessage () {
		List<Message> messages;
		while (true) {
			messages = inboundQueueFromManager.waitForMessages(1);
			if (messages != null) {
				System.out.println("");
				QueueUtil.debugMessagesForMe(messages);
				return messages.get(0);
			}
			try {
				System.out.print(".");
				Thread.sleep(1000);
			} catch (InterruptedException e) {}
		}		
	}
	
	private static boolean startUpRemoteManager (String n) {
		try {
			remoteManagerInstance = ec2.getManagerInstance();
			if (null == remoteManagerInstance) {
				String managerStartupScript = UserDataScriptsClass.getManagerStartupScript(
						MANAGER_JAR_NAME,
						MANAGER_JAR_MAIN_CLASS,
						MANAGER_JAR_PARAMETERS);
				remoteManagerInstance = ec2.createNode(1, managerStartupScript).get(0);
				ec2.setManagerTag(remoteManagerInstance);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	private static boolean shutDownRemoteManager () {
		ec2.removeManagerTag(remoteManagerInstance);
		ec2.terminateMachine(remoteManagerInstance);
		return true;
	}
}
