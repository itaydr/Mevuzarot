package task1;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.HashMap;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.QueueDeletedRecentlyException;

public class QueueUtil {

	// Messages type
	public static final String MSG_START_JOB 		= "start_job";	// local --> manager
	public static final String MSG_JOB_ENDED 		= "job_ended";	// manager --> local
	public static final String MSG_TERMINATE		= "terminate";	// local --> manager --> workers
	public static final String MSG_FINISHED_WORK 	= "thumbnail_ended"; // worker --> manager
	public static final String MSG_INIT_WORK 		= "init_url_thumb";	// manager --> worker
	public static final String MSG_ERROR_WORK 		= "error_work";

	// UIDs of endpoints
	private static String REMOTE_MANAGER_IDENTIFIER = "remote_manager";
	private static String BROADCAST = "*";
	
    public String currentUID;
	
	private AmazonSQS sqs;
	private String queueUrl;
	private String queueName;
	
	public QueueUtil (PropertiesCredentials creds, String queueNameToConnectTo, String NodeId) {
		super();
		if ( null == NodeId ) {
			currentUID = "" + UUID.randomUUID();
		} else {
			currentUID = NodeId;
		}
		queueName = queueNameToConnectTo;
		sqs = new AmazonSQSClient(creds);

		this.initQueue();
	}
	
	private void initQueue () {

		try {
			GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(queueName);
			this.queueUrl = sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();
		}
		catch (QueueDoesNotExistException ex) {
			this.queueUrl = null;
		}
		catch (QueueDeletedRecentlyException ex) {
			this.queueUrl = null;
		}
        
        if (this.queueUrl == null) {
    		CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            this.queueUrl = myQueueUrl;
        }
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
		
	public List<Message> waitForMessages(int maxNumOfMessagesForMe) {
		return private_waitForMessages(maxNumOfMessagesForMe);
	}
	
	public List<Message> waitForMessages() {
		return private_waitForMessages(10);
	}
	
	private List<Message> private_waitForMessages(int maxNumOfMessagesForMe) {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
		receiveMessageRequest = receiveMessageRequest.withQueueUrl(queueUrl);
		receiveMessageRequest = receiveMessageRequest.withMessageAttributeNames(".*");
		receiveMessageRequest = receiveMessageRequest.withMaxNumberOfMessages(10);
		receiveMessageRequest = receiveMessageRequest.withVisibilityTimeout(1);

		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        
        if (messages == null || messages.isEmpty()) return null;
        
        List<Message> messagesForMe = new ArrayList<Message>();
        int i = 0;
        for (Message msg : messages) {
        	if (i >= maxNumOfMessagesForMe)
        		break;
        	MessageAttributeValue to = msg.getMessageAttributes().get("to");
        	if (to != null && to.getStringValue().equals(currentUID)) {
        		messagesForMe.add(msg);
        		sqs.deleteMessage(new DeleteMessageRequest().withQueueUrl(queueUrl).withReceiptHandle(msg.getReceiptHandle()));
        		i++;
        	}
        }
        
        
        //QueueManager.printMessages(messagesForMe);
        if (messagesForMe.size() == 0) {
        	return null;
        }
        
        return messagesForMe;
	}
	
	public Message getSingleBroadcastMessage(int WORKER_TIMEOUT) {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
		receiveMessageRequest = receiveMessageRequest.withQueueUrl(queueUrl);
		receiveMessageRequest = receiveMessageRequest.withMessageAttributeNames(".*");
		receiveMessageRequest = receiveMessageRequest.withVisibilityTimeout(WORKER_TIMEOUT);
		receiveMessageRequest = receiveMessageRequest.withMaxNumberOfMessages(1);

		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		
		if (messages.size() == 0) {
			return null;
		}
		
        return messages.get(0);
	}
	
	public static void printMessages (List<Message> messages) {
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
	
	public static void debugMessagesForMe (List<Message> messagesForMe) {
		for (Message msg : messagesForMe) {
            System.out.println("  Message:");
            //System.out.println("    MessageId:     " + message.getMessageId());
            //System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
            //System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
            //System.out.println("    Body:          " + message.getBody());
            System.out.println("    From:          " + msg.getMessageAttributes().get("from").getStringValue());
            System.out.println("    to:          " + msg.getMessageAttributes().get("to").getStringValue());
            System.out.println("    type:          " + msg.getMessageAttributes().get("type").getStringValue());
            System.out.println("    data:          " + msg.getBody());
        }
	}
	
	public int queryNumberOfMessagesInQueue() {
		GetQueueAttributesRequest a = new GetQueueAttributesRequest().withQueueUrl(queueUrl).withAttributeNames("ApproximateNumberOfMessages");
		Map<String, String> result = sqs.getQueueAttributes(a).getAttributes();
		//System.out.println(result.get(ApproximateNumberOfMessages));
		return Integer.parseInt(result.get("ApproximateNumberOfMessages"));
	}
	
	public void killQueue() {
		sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
	}
	
	public void deleteMessageFromQueue(Message message) {
		sqs.deleteMessage(new DeleteMessageRequest().withQueueUrl(queueUrl).withReceiptHandle(message.getReceiptHandle()));
	}
	
	// ------------------------------ SEND MESSAGES -------------------------------------
	public void startJobWithFile(String pathInS3) {
		this.sendMessage(pathInS3, currentUID, REMOTE_MANAGER_IDENTIFIER, MSG_START_JOB);
	}
	
	public void finishJob(String summaryPathInS3, String localMachine) {
		this.sendMessage(summaryPathInS3, REMOTE_MANAGER_IDENTIFIER, localMachine, MSG_JOB_ENDED);
	}
	
	
	public void sendTerminationSignal() {
		this.sendMessage(MSG_TERMINATE, currentUID, REMOTE_MANAGER_IDENTIFIER, MSG_TERMINATE);
	}
	
	public void finishWorkNotify(String origURL, String pathInS3) {
		this.sendMessage(origURL + ";" + pathInS3, currentUID, REMOTE_MANAGER_IDENTIFIER, MSG_FINISHED_WORK);
	}

	public void sendSingleURLWork(String url) {
		this.sendMessage(url, REMOTE_MANAGER_IDENTIFIER, BROADCAST, MSG_INIT_WORK);
	}

	public void sendTerminationToWorks() {
		this.sendMessage(MSG_TERMINATE, REMOTE_MANAGER_IDENTIFIER, BROADCAST, MSG_TERMINATE);
	}

	public void sendErrorToManager(String url) {
		this.sendMessage(url, currentUID, REMOTE_MANAGER_IDENTIFIER, MSG_ERROR_WORK);		
	}

	public void sendTerminationACK(String mLocalMachineACK) {
		this.sendMessage(MSG_TERMINATE, REMOTE_MANAGER_IDENTIFIER, mLocalMachineACK, MSG_TERMINATE);
	}
}
