package task1.work;

import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.HashMap;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.QueueDeletedRecentlyException;

public class QueueManager {

	public static String START_JOB_TYPE_KEY = "start_job";
	public static String JOB_ENDED_TYPE_KEY = "job_ended";
	public static String TERMINATE_TYPE_KEY	= "terminate";
	public static String REMOTE_MANAGER_KEY = "remote_manager";
	private static String LOCAL_QUEUE_IDENTIFIER = "mevuzarot_task1_localqueue";
	private static QueueManager sInstance;
	private static Object LOCK = new Object();
	
	private AmazonSQS sqs;
	private String queueUrl;
	private String uid;
	private String identifier;
	
	public static QueueManager sharedInstance () {
		if (sInstance == null) {
			synchronized(LOCK) {
				if (sInstance == null) {
					sInstance = new QueueManager(LOCAL_QUEUE_IDENTIFIER);
				}
			}
		}
		
		return sInstance;
	}
	
	public QueueManager (String queueIdentifier) {
		super();
		uid = "" + UUID.randomUUID();
		identifier = queueIdentifier;
	}
	
	public void init(PropertiesCredentials creds) {
		sqs = new AmazonSQSClient(creds);
		
		this.initLocalQueue();
	}
	
	private void initLocalQueue () {

		try {
			GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(identifier);
			this.queueUrl = sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();
		}
		catch (QueueDoesNotExistException ex) {
			this.queueUrl = null;
		}
		catch (QueueDeletedRecentlyException ex) {
			this.queueUrl = null;
		}
        
        if (this.queueUrl == null) {
    		CreateQueueRequest createQueueRequest = new CreateQueueRequest(identifier);
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            this.queueUrl = myQueueUrl;
        }
	}
	
	public void startJobWithFile(String pathInS3) {
		this.sendMessage(pathInS3, uid, REMOTE_MANAGER_KEY, START_JOB_TYPE_KEY);
	}
	
	public void sendMessage(String message,String from, String to, String type) {
		
		HashMap<String, MessageAttributeValue> messageAttributes = new HashMap<>();
		if (from!=null)
			messageAttributes.put("from", new MessageAttributeValue().withDataType("String").withStringValue(from));
		if (to !=null)
			messageAttributes.put("to", new MessageAttributeValue().withDataType("String").withStringValue(to));
		if (type != null)
			messageAttributes.put("type", new MessageAttributeValue().withDataType("String").withStringValue(type));
		
		SendMessageRequest send = new SendMessageRequest(queueUrl, message);
		send.withMessageAttributes(messageAttributes);
			
		sqs.sendMessage(send);
	}
		
	public void terminate() {
		this.sendMessage(TERMINATE_TYPE_KEY, uid, REMOTE_MANAGER_KEY, null);
	}
	
	public void killQueue() {
		sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
	}
	
	public List<Message> waitForMessages() {

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
		receiveMessageRequest.withQueueUrl(queueUrl);
		receiveMessageRequest.withMessageAttributeNames(".*");

		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        
        if (messages == null || messages.isEmpty()) return null;
        
        List<Message> messagesForMe = new ArrayList<Message>();
        for (Message msg : messages) {
        	MessageAttributeValue to = msg.getMessageAttributes().get("to");
        	if (to != null && to.getStringValue().equals(uid)) {
        		messagesForMe.add(msg);
        		sqs.deleteMessage(new DeleteMessageRequest().withQueueUrl(queueUrl).withReceiptHandle(msg.getReceiptHandle()));
        	}
        }
        
        
        QueueManager.printMessages(messagesForMe);
        
        return messagesForMe;
	}
	
	private static void printMessages (List<Message> messages) {
		for (Message message : messages) {
            System.out.println("  Message");
            System.out.println("    MessageId:     " + message.getMessageId());
            System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
            System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
            System.out.println("    Body:          " + message.getBody());
            for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                System.out.println("  Attribute");
                System.out.println("    Name:  " + entry.getKey());
                System.out.println("    Value: " + entry.getValue());
            }
        }
	}
	
}
