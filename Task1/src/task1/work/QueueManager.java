package task1.work;

import java.util.List;
import java.util.UUID;
import java.util.Map.Entry;

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
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class QueueManager {

	private static String QUEUE_IDENTIFIER = "mevuzarot.task1.localqueue";
	private static QueueManager sInstance;
	private static Object LOCK = new Object();
	
	private AmazonSQS sqs;
	private String localQueueUrl;
	
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
		
	}
	
	public void init(PropertiesCredentials creds) {
		sqs = new AmazonSQSClient(creds);
		
		this.initLocalQueue();
	}
	
	private void initLocalQueue () {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_IDENTIFIER);
        String addy = sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();
        
        if (addy == null) {
    		CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_IDENTIFIER);
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            this.localQueueUrl = myQueueUrl;
        }
        else {
        	this.localQueueUrl = addy;
        }
	}
	
	public void startJobWithFile(String pathInS3) {
		this.sendMessage(pathInS3);
	}
	
	private void sendMessage(String message) {
		sqs.sendMessage(new SendMessageRequest(localQueueUrl, message));
	}
	
	public void terminate() {
		this.sendMessage("terminate");
	}
	
	public void killQueue() {
		sqs.deleteQueue(new DeleteQueueRequest(localQueueUrl));
	}
	
	public List<Message> waitForMessages() {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(localQueueUrl);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        
        QueueManager.printMessages(messages);
        
        return messages;
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
