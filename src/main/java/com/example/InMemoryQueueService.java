package com.example;

import com.google.common.collect.*;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

public class InMemoryQueueService implements QueueService {
  //
  // Task 2: Implement me.
  //
	private Multimap<String, QueueMessage> queues;
	private long visibilityTimeout = 30;
	
	private static final Logger log = Logger.getLogger("InMemoryQueueService");
	
	public InMemoryQueueService() {
		queues = Multimaps.synchronizedListMultimap(LinkedListMultimap.create());
	}
	
	/*
	 * Sets visibilityTimeout to be applied to new messages
	 */
	public void setVisibilityTimeout(long visibilityTimeout) {
		this.visibilityTimeout = visibilityTimeout;
	}
	
	/*
	 * Pushes a new message to a queue specified by queueName with content msgContent 
	 */
	@Override
	public void push(String queueName, String msgContent) {
		Collection<QueueMessage> queue = queues.get(queueName);
		queue.add(new QueueMessage(msgContent, visibilityTimeout));
	}
	
	/*
	 * Pulls a message from specified queue in (attempted) FIFO order. Returns null 
	 * if queue does not exist or if no messages are visible in queue.
	 */
	@Override
	public QueueMessage pull(String queueName) {
		Collection<QueueMessage> queue = queues.get(queueName);
		if(queue.isEmpty()) {
			log.warning("Cannot pull messages from " + queueName + " as queue does not exist");
			return null;
		}
		try {
			QueueMessage message = Iterables.find(queue, msg -> msg.isVisible());
			message.setTimeoutInMillis();
		
		    return message; 
		    
		} catch (NoSuchElementException e) {
			log.warning("No Visible Messages in Queue " + queueName);
		    return null;
		}
	}
	
	/*
	 * Deletes message if visibility timeout has not expired. If it has, does not delete.
	 * Also does not delete if invalid queue, message, or message is in a different queue
	 * than selected.
	 */
	@Override
	public void delete(String queueName, Object msg) {
		Collection<QueueMessage> queue = queues.get(queueName);
		if(queue.isEmpty()) {
			log.warning("Cannot delete messages from " + queueName + " as queue does not exist");
			return;
		}
		if(!(msg instanceof QueueMessage)) {
			log.warning("Invalid message, cannot delete");
			return;
		}
		QueueMessage qMsg = (QueueMessage) msg;
		if(!qMsg.isVisible()) {
			boolean removed = queue.remove(qMsg);
			if(!removed) {
				log.warning("Message could not be deleted from queue " + queueName);
			}
		}
		else {
			log.warning("Message does not exist or visibility timeout has expired");
		}
	}
	
	public int getQueueSize(String queueName) {
		Collection<QueueMessage> queue = queues.get(queueName);
		return queue.size();
	}
}
