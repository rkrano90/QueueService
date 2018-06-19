package com.example;

import com.google.common.base.Joiner;

public class QueueMessage {
	private String msgContent;
	private long visibilityTimeout;
	private long timeoutForMsg;
	
	QueueMessage(String msgContent, long visibilityTimeout){
		this.msgContent = msgContent;
		this.visibilityTimeout = visibilityTimeout;
		this.timeoutForMsg = 0;
	}
	
	QueueMessage(String msgContent, long visibilityTimeout, long timeoutForMsg){
		this.msgContent = msgContent;
		this.visibilityTimeout = visibilityTimeout;
		this.timeoutForMsg = timeoutForMsg;
	}
	
	public long getVisibilityTimeout() {
		return visibilityTimeout;
	}
	
	public void setTimeoutInMillis() {
		timeoutForMsg = System.currentTimeMillis() + (visibilityTimeout * 1000);
	}
	
	public long getTimeoutInMillis() {
		return timeoutForMsg;
	}
	
	public String getContent() {
	    return msgContent;
	}
	
	public boolean isVisible() {
		return System.currentTimeMillis() > timeoutForMsg;
	}
	
	public String toString() {
		return Joiner.on(":").join(timeoutForMsg, msgContent, visibilityTimeout);
	}
	
	@Override
	public boolean equals(Object obj) {
	    if (obj == null) return false;
	    if (obj == this) return true;
	    if (!(obj instanceof QueueMessage)) return false;
	    QueueMessage msg = (QueueMessage) obj;
	    if(msg.getContent().equals(this.getContent()) && msg.getTimeoutInMillis() == this.getTimeoutInMillis()) {
	    	return true;
	    }
	    return false;
	}

}
