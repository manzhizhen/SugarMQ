package com.sugarmq.queue;

import javax.jms.JMSException;
import javax.jms.Queue;

import com.sugarmq.message.SugarDestination;

public class SugarQueue extends SugarDestination implements Queue {
	private static final long serialVersionUID = 3731874591969734047L;
	
	public SugarQueue() {
	}
	
	public SugarQueue(String queueName) {
		this.name = queueName;
	}

	@Override
	public String getQueueName() throws JMSException {
		return name;
	}

	public void setQueueName(String queueName) {
		this.name = queueName;
	}

}
