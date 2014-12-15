package com.sugarmq.topic;

import javax.jms.JMSException;
import javax.jms.Topic;

import com.sugarmq.message.SugarDestination;

public class SugarTopic extends SugarDestination implements Topic {

	private static final long serialVersionUID = -3220447059854100741L;

	@Override
	public String getTopicName() throws JMSException {
		return name;
	}

}
