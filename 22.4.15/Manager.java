package task1;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;


public class Manager {

	private static PropertiesCredentials Credentials;
	private final static String propertiesFilePath = "/home/asaf/Desktop/Mevuzarot/creds/asaf";
	
	private static final String WORKER_JAR_NAME = "Worker.jar";
	private static final String WORKER_JAR_MAIN_CLASS = "task1.Worker";
	private static final String WORKER_JAR_PARAMETERS = "";
	
	private static String mWorkerStartupScript;
	
	
	private static QueueUtil inboundQueueFromLocalAndWorkers;
	private static QueueUtil outboundQueueToWorkers;
	private static QueueUtil outboundQueueToLocalMachines;
	
	private final static String TO_LOCAL_QUEUE_IDENTIFIER 	= "mevuzarot_task1_to_local";
	private final static String TO_MANAGER_QUEUE_IDENTIFIER = "mevuzarot_task1_to_manager";
	private final static String TO_WORKERS_QUEUE_IDENTIFIER = "mevuzarot_task1_to_workers";
	
	private final static String REMOTE_MANAGER_IDENTIFIER = "remote_manager";
	private final static String bucketName = "mevuzarot.task1";
	private static boolean mTerminated = false;

	
	private static EC2Util ec2;
	private static S3Util s3_client;
	private static ArrayList<Instance> workers;
	private static ArrayList<Job> jobs;
	private static int n;
	
	@SuppressWarnings("unused")
	private static int currentRunningURLs;
	private static String mLocalMachineACK;
	
	private static class Job {
		private String localMachineID;
		private HashMap<String, Boolean> urlListController;
		private ArrayList<String> outputSummary;
		private int unfinishedURLs;
		
		Job(String localMachineID, List<String> urlList) {
			this.localMachineID = localMachineID;
			this.urlListController = new HashMap<String, Boolean>();
			this.outputSummary = new ArrayList<String>();
			for (String url : urlList) {
				this.urlListController.put(url, false);
			}
			this.unfinishedURLs = urlList.size();
			Manager.currentRunningURLs += urlList.size();
		}
		
		@Override
		public boolean equals(Object obj) {
			if ( ! (obj instanceof  Job)) return false;
			Job other = (Job) obj;
			if ( ! ( localMachineID.equals(other.localMachineID) )) return false;
			if ( ! ( urlListController.equals(other.urlListController ))) return false;
			if ( ! ( outputSummary.equals(other.outputSummary))) return false;
			if ( ! ( unfinishedURLs == other.unfinishedURLs)) return false;
			
			return true;
		}
		
		public boolean isURLinJob(String url){
			return urlListController.containsKey(url);
		}
		
		public void markDoneURL(String origURL, String fullLine){
			urlListController.put(origURL, true);
			outputSummary.add(fullLine);
			unfinishedURLs--;
			Manager.currentRunningURLs--;
		}
		
		public void markErrorURL(String url){
			urlListController.put(url, null);
			unfinishedURLs--;
			Manager.currentRunningURLs--;
		}
		
		public ArrayList<String> getSummary(){
			return outputSummary;
		}
		
		public int getUnfinishedURLs() {
			return unfinishedURLs;
		}
		
		public String getLocalMachineID() {
			return localMachineID;
		}
	}
	
	public static void main(String[] args) throws InterruptedException {
		if (args.length == 0) {
			System.out.println("Must provide arguments for input and output");
			return;
		}
		
		// init variables
		n = Integer.parseInt(args[0]);
		currentRunningURLs = 0;
		jobs = new ArrayList<Job>();
		workers = new ArrayList<Instance>();
		mWorkerStartupScript = UserDataScriptsClass.getManagerStartupScript(WORKER_JAR_NAME, 
				WORKER_JAR_MAIN_CLASS, 
				WORKER_JAR_PARAMETERS);

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
		inboundQueueFromLocalAndWorkers = new QueueUtil(Credentials, TO_MANAGER_QUEUE_IDENTIFIER, REMOTE_MANAGER_IDENTIFIER);
		outboundQueueToWorkers =  new QueueUtil(Credentials, TO_WORKERS_QUEUE_IDENTIFIER, REMOTE_MANAGER_IDENTIFIER);
		outboundQueueToLocalMachines =  new QueueUtil(Credentials, TO_LOCAL_QUEUE_IDENTIFIER, REMOTE_MANAGER_IDENTIFIER);
		s3_client = new S3Util(Credentials, bucketName);
		ec2 = new EC2Util(Credentials);
		
		while (false == mTerminated || jobs.size() != 0) {
			List<Message> messages = inboundQueueFromLocalAndWorkers.waitForMessages();
			if (messages != null) {
				System.out.println("");
				QueueUtil.debugMessagesForMe(messages);
				for (Message msg : messages) {
					if (false == handleMessage(msg)) {
						System.out.println("terminated or failed to parse message, quitting");
						return;
					}
				}
			} else {
				try {
					System.out.print(".");
					Thread.sleep(1 * 1000); // sleep 1 sec
				} catch (InterruptedException e) {}
			}
		}
		
		// Terminate all workers
		System.out.println("Sending termination signals to workers...");
		for (int i = 0; i < workers.size(); i++) {
			outboundQueueToWorkers.sendTerminationToWorks();
		}
			
		Thread.sleep(5 * 1000); // sleep 5 sec to let the queue refresh
		System.out.println("waiting for workers to terminate...");	
		while ( 0 != outboundQueueToWorkers.queryNumberOfMessagesInQueue() ){
			Thread.sleep(5 * 1000); // sleep 5 sec
		}
		System.out.println("all got tremination signals, waiting 30 seconds and strating to terminate machine");
		Thread.sleep(30 * 1000); // sleep for 30 sec
		

		for ( Instance i : workers ) {
			ec2.terminateMachine(i);
		}
		
		System.out.println("Sending shutdown ACK to local machine");
		outboundQueueToLocalMachines.sendTerminationACK(mLocalMachineACK);
		
		System.out.println("shutting down..");
		return;
	}
	
	private static boolean handleMessage(Message msg) {
		MessageAttributeValue value = msg.getMessageAttributes().get("type");
		if (value == null) {
			System.out.println("ERROR: no type in message!!");
			return false;
		}
		
		String type = value.getStringValue();
		if (type.equals(QueueUtil.MSG_TERMINATE)) {
			mTerminated = true;
			mLocalMachineACK = msg.getMessageAttributes().get("from").getStringValue();
		}
		else if (type.equals(QueueUtil.MSG_START_JOB)) {
			String urlListinS3 		= msg.getBody();
			String localMachineID 	= msg.getMessageAttributes().get("from").getStringValue();
			
			if (mTerminated) { //we are terminated (should not get here...)
				System.out.println("Cannot start new job - terminated");
				outboundQueueToLocalMachines.sendMessage(QueueUtil.MSG_TERMINATE, REMOTE_MANAGER_IDENTIFIER, localMachineID, QueueUtil.MSG_TERMINATE);
			}
			else { // we are running
				List<String> urls = s3_client.getFileContentFromS3(urlListinS3);
				Job new_job = new Manager.Job(localMachineID, urls);
				jobs.add(new_job);
				
				for (String url : urls) {
					outboundQueueToWorkers.sendSingleURLWork(url);
				}
				
				int numOfPendingMessages = outboundQueueToWorkers.queryNumberOfMessagesInQueue();
				
				if ( numOfPendingMessages > (n * workers.size()) ) {
					int capacityDiff = numOfPendingMessages - n * workers.size();
					if (capacityDiff > 0) {
						int newWorkersToCreate = capacityDiff / n;
						if ( capacityDiff % n != 0 ) {
							newWorkersToCreate +=1;
						}
						System.out.println("should create: " + newWorkersToCreate + " new workers");
						workers.addAll(ec2.createNode(newWorkersToCreate,mWorkerStartupScript));
					}
				}
			}
		}
		else if (type.equals(QueueUtil.MSG_FINISHED_WORK)) {
			for (Job j : jobs) {
				String origURL  = msg.getBody().substring(0, msg.getBody().indexOf(';'));
				if ( j.isURLinJob(origURL) ){
					j.markDoneURL(origURL , msg.getBody());
					if ( 0 == j.getUnfinishedURLs() ) { //job is done!
						createSummaryAndReplyLocal(j);
						jobs.remove(j);
					}
					break;
				}
			}
		} else if (type.equals(QueueUtil.MSG_ERROR_WORK)) {
			for (Job j : jobs) {
				String origURL  = msg.getBody();
				if ( j.isURLinJob(origURL) ){
					j.markErrorURL(origURL);
					if ( 0 == j.getUnfinishedURLs() ) { //job is done!
						createSummaryAndReplyLocal(j);
						jobs.remove(j);
					}
					break;
				}
			}
		}
		return true;
	}
	

	private static void createSummaryAndReplyLocal(Job j) {
		ArrayList<String> summary= j.getSummary();
		String summaryFilePath = "/tmp/" + j.getLocalMachineID(); 

		try {
			// write summary file
			File file = new File (summaryFilePath);
			PrintWriter out = new PrintWriter(file);
			for (String line : summary) {
				out.println(line);
			}
			out.close();
			
			// upload to S3 summary file
			String summaryPathInS3 = s3_client.uploadFileToS3(summaryFilePath);
			
			// send finished job message to local machine
			System.out.println("Sending finishJob to: " + j.getLocalMachineID());
			outboundQueueToLocalMachines.finishJob(summaryPathInS3, j.getLocalMachineID());
			
			// delete uploaded file
			removeFile(summaryFilePath);
		} catch (FileNotFoundException e) {
			System.out.println("Failed to write to file: "+ summaryFilePath);
			for (String line : summary) {
				System.out.println(line);								
			}
		}
	}

	private static void removeFile(String filePath) {
		
		File file = new File(filePath);
		try {
		    if ( file.delete()) {
		    	System.out.println("file deleted: "+ filePath);
		    } else {
		    	System.out.println("Failed to delete File: "+ filePath);
		    }
		} catch (Exception x) {
			System.out.println("Failed to delete File: "+ filePath);
		}
	}

}