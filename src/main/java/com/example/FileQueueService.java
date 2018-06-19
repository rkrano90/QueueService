package com.example;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;

public class FileQueueService implements QueueService {
  //
  // Task 3: Implement me if you have time.
  //
	
	private Path rootPath;
	private long visibilityTimeout = 30;
	
	private static final Logger log = Logger.getLogger("FileQueueService");
    
    public FileQueueService(String rootPath, long visibilityTimeout) throws IOException {
        this.rootPath = Paths.get(rootPath);
        this.visibilityTimeout = visibilityTimeout;
        if (!Files.isDirectory(this.rootPath, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException(this.rootPath + " is not a valid directory");
        }
    }
	
    @Override
	public void push(String queueName, String msgContent) throws IOException{
		String message = new QueueMessage(msgContent, visibilityTimeout).toString() + "\n";
		Path queuePath = getQueuePath(queueName);
		Path messagePath = getMessagePath(queuePath);
		File lock = queuePath.resolve(".lock").toFile();
		
		try {
			lock(lock);
			Files.write(messagePath, message.getBytes(), StandardOpenOption.APPEND);
		} catch (Exception e) {
			log.warning("Error occurred pushing message " + msgContent + " to queue " + queueName + "; Error: " + e);
		} finally {
			unlock(lock);
		}
	}
	
    @Override
	public QueueMessage pull(String queueName) throws IOException{
    	Path queuePath = getQueuePath(queueName);
		Path messagePath = getMessagePath(queuePath);
		File lock = queuePath.resolve(".lock").toFile();
		QueueMessage message = null;
		
		try {
			lock(lock);
			List<QueueMessage> qmsgList = getAllMessages(messagePath);
			message = Iterables.find(qmsgList, msg -> msg.isVisible());
			message.setTimeoutInMillis();
			writeAllMessages(messagePath, qmsgList);
		} catch (Exception e) {
			log.warning("Error occurred pulling message from queue " + queueName + "; Error: " + e);
		} finally {
			unlock(lock);
		}
		
		return message;
	}
    
    @Override
    public void delete(String queueName, Object msg) throws IOException {
    	Path queuePath = getQueuePath(queueName);
		Path messagePath = getMessagePath(queuePath);
		File lock = queuePath.resolve(".lock").toFile();
		
		if(!(msg instanceof QueueMessage)) {
			log.warning("Invalid message, cannot delete");
			return;
		}
		QueueMessage qMsg = (QueueMessage) msg;
		
		if(!qMsg.isVisible()) {
			try {
				lock(lock);
				List<QueueMessage> qmsgList = getAllMessages(messagePath);
				boolean removed = qmsgList.remove(qMsg);
				if(!removed) {
					log.warning("Message could not be removed from Queue " + queueName);
				}
				writeAllMessages(messagePath, qmsgList);
			} catch (Exception e) {
				log.warning("Error occurred deleting message from queue " + queueName + "; Error: " + e);
			} finally {
				unlock(lock);
			}
		}
    }
    
    private void writeAllMessages(Path messagePath, List<QueueMessage> qmsgList) throws IOException {
	    String msgs = Joiner.on('\n').join(qmsgList);
	    Files.write(messagePath, msgs.getBytes());
    }
	
	private Path getQueuePath(String queueName) throws IOException {
		Path queuePath = rootPath.resolve(queueName);
		if (Files.notExists(queuePath, LinkOption.NOFOLLOW_LINKS)) {
            Files.createDirectories(queuePath);
        }
		return queuePath;
	}
	
	private Path getMessagePath(Path queuePath) throws IOException {
		Path messagePath = queuePath.resolve("messages");
		if (Files.notExists(messagePath, LinkOption.NOFOLLOW_LINKS)) {
            Files.createFile(messagePath);
        }
		return messagePath;
	}
	
	private List<QueueMessage> getAllMessages(Path messagePath) throws IOException {
		List<String> lines = Files.readAllLines(messagePath, StandardCharsets.UTF_8);
		List<QueueMessage> msgList = new ArrayList<QueueMessage>();
		for(String line : lines) {
			Iterable<String> messageInfo = Splitter.on(':').split(line);
		    Long timeoutForMsg = Long.parseLong(Iterables.get(messageInfo, 0), 10);
		    String msgContent = Iterables.get(messageInfo, 1);
		    Long visibilityTimeout = Long.parseLong(Iterables.get(messageInfo, 2), 10);
		
		    msgList.add(new QueueMessage(msgContent, visibilityTimeout, timeoutForMsg));
		}
		return msgList;
	}
	
    private void lock(File lock) throws InterruptedException {
        while (!lock.mkdir()) {
            Thread.sleep(50);
        }
    }

    private void unlock(File lock) {
        lock.delete();
    }
    
    /*
	 * Sets visibilityTimeout to be applied to new messages
	 */
	public void setVisibilityTimeout(long visibilityTimeout) {
		this.visibilityTimeout = visibilityTimeout;
	}
	
    public int getQueueSize(String queueName) throws IOException {
        Path messagesPath = getQueuePath(queueName).resolve("messages");
        return Files.readAllLines(messagesPath).size();
    }
}
