package com.sugarmq.message;

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;

public class SugarDestination implements Queue, Topic, Serializable {

	private static final long serialVersionUID = 4315929928684782158L;
	
	protected String name;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String getTopicName() throws JMSException {
		return name;
	}

	@Override
	public String getQueueName() throws JMSException {
		return name;
	}
	
	
}
