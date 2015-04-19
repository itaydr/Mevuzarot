package task1.work;

import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.HashMap;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
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

	private static String QUEUE_IDENTIFIER = "mevuzarot_task1_localqueue";
	private static QueueManager sInstance;
	private static Object LOCK = new Object();
	
	private AmazonSQS sqs;
	private String localQueueUrl;
	private String uid;
	
	public static QueueManager sharedInstance () {
		if (sInstance == null) {
			synchronized(LOCK) {
				if (sInstance == null) {
					sInstance = new QueueManager();
				}
			}
		}
		
		return sInstance;
	}
	
	public QueueManager () {
		uid = "" + UUID.randomUUID();
	}
	
	public void init(PropertiesCredentials creds) {
		sqs = new AmazonSQSClient(creds);
		
		this.initLocalQueue();
	}
	
	private void initLocalQueue () {

		try {
			GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_IDENTIFIER);
			this.localQueueUrl = sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();
		}
		catch (QueueDoesNotExistException ex) {
			this.localQueueUrl = null;
		}
		catch (QueueDeletedRecentlyException ex) {
			this.localQueueUrl = null;
		}
        
        if (this.localQueueUrl == null) {
    		CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_IDENTIFIER);
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            this.localQueueUrl = myQueueUrl;
        }
	}
	
	public void startJobWithFile(String pathInS3) {
		this.sendMessage(pathInS3);
	}
	
	private void sendMessage(String message) {
		
		HashMap<String, MessageAttributeValue> messageAttributes = new HashMap<>();
		messageAttributes.put("from", new MessageAttributeValue().withDataType("String").withStringValue(uid));
		messageAttributes.put("to", new MessageAttributeValue().withDataType("String").withStringValue("remoteManager"));
		
		SendMessageRequest send = new SendMessageRequest(localQueueUrl, message);
		send.withMessageAttributes(messageAttributes);
			
		sqs.sendMessage(send);
	}
	
	public void terminate() {
		this.sendMessage("terminate");
	}
	
	public void killQueue() {
		sqs.deleteQueue(new DeleteQueueRequest(localQueueUrl));
	}
	
	public List<Message> waitForMessages() {

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
		receiveMessageRequest.withQueueUrl(localQueueUrl);
		receiveMessageRequest.withMessageAttributeNames(".*");

		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        
        if (messages == null || messages.isEmpty()) return null;
        
        List<Message> messagesForMe = new ArrayList<Message>();
        for (Message msg : messages) {
        	MessageAttributeValue to = msg.getMessageAttributes().get("to");
        	if (to != null && to.getStringValue().equals(uid)) {
        		messagesForMe.add(msg);
        		sqs.deleteMessage(new DeleteMessageRequest().withQueueUrl(localQueueUrl).withReceiptHandle(msg.getReceiptHandle()));
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
