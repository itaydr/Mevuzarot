package task1;

import java.util.Random;

public class Config {
	public final static String propertiesFilePath = "credentials.file";
//	public final static String propertiesFilePath = "/home/asaf/Desktop/Mevuzarot/outputJars/runnable/credentials.file";
	
	// JAR
	public static final String TASK1_JAR_NAME = "task1.jar";
	public static final String MANAGER_JAR_MAIN_CLASS = "task1.Manager";
	public static final String WORKER_JAR_MAIN_CLASS = "task1.Worker";
	public static final String WORKER_JAR_PARAMETERS = "";
	
	// S3
	public final static String bucketName = "mevuzarot.task1";
	
	// sqs
	public final static String TO_LOCAL_QUEUE_IDENTIFIER 		= "mevuzarot_task1_to_local";
	public final static String TO_MANAGER_QUEUE_IDENTIFIER 	= "mevuzarot_task1_to_manager";
	public final static String TO_WORKERS_QUEUE_IDENTIFIER   = "mevuzarot_task1_to_workers";
	
	public final static String REMOTE_MANAGER_IDENTIFIER = "remote_manager";

	

	public static int WORKER_TIMEOUT = 60 * 3; // 3 min per picture

	public static final String workerLogFilePath = "/tmp/worker.log";
	public static final String ManagerLogFilePath = "/tmp/manager.log";
	
	public static int randomSleep(int min, int max) {
		Random r = new Random();
		return r.nextInt(max-min) + min;
	}

}
