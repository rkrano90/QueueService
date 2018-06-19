package com.example;

import java.util.List;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class SqsQueueService implements QueueService {
  //
  // Task 4: Optionally implement parts of me.
  //
  // This file is a placeholder for an AWS-backed implementation of QueueService.  It is included
  // primarily so you can quickly assess your choices for method signatures in QueueService in
  // terms of how well they map to the implementation intended for a production environment.
  //
	private AmazonSQSClient sqsClient;

	public SqsQueueService(AmazonSQSClient sqsClient) {
		this.sqsClient = sqsClient;
	}
	
	public void push(String queueName, String msgContent) {
	    GetQueueUrlResult queueUrl = sqsClient.getQueueUrl(queueName);
	    sqsClient.sendMessage(new SendMessageRequest(queueUrl.getQueueUrl(), msgContent));
	}
	 
	public Object pull(String queueName) {
		GetQueueUrlResult queueUrl = sqsClient.getQueueUrl(queueName);
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl.getQueueUrl());
		List<Message> msgs = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
		return msgs.get(0);
	}

	public void delete(String queueName, Object msg) {
	    GetQueueUrlResult queueUrl = sqsClient.getQueueUrl(queueName);
	    Message sqsMsg = (Message)msg;
	    sqsClient.deleteMessage(new DeleteMessageRequest(queueUrl.getQueueUrl(), sqsMsg.getReceiptHandle()));
	}
}
