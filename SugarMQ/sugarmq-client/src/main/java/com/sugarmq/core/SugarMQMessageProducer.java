/**
 * 
 */
package com.sugarmq.core;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

/**
 * 抽象的消息生产者
 * 
 * @author manzhizhen
 * 
 */
public class SugarMQMessageProducer implements QueueSender, TopicPublisher {
	protected volatile AtomicInteger deliveryMode = new AtomicInteger(Message.DEFAULT_DELIVERY_MODE); // 持久性和非持久性
	protected volatile AtomicLong timeToLive = new AtomicLong(Message.DEFAULT_TIME_TO_LIVE); // 消息有效期
	protected volatile AtomicInteger priority = new AtomicInteger(Message.DEFAULT_PRIORITY);	// 消息优先级

	protected volatile AtomicBoolean disableMessageId = new AtomicBoolean(false);
	
	@Override
	public int getDeliveryMode() throws JMSException {
		return deliveryMode.get();
	}

	@Override
	public int getPriority() throws JMSException {
		return priority.get();
	}

	@Override
	public long getTimeToLive() throws JMSException {
		return timeToLive.get();
	}

	@Override
	public void send(Message arg0) throws JMSException {
		// TODO Auto-generated method stub
	}

	@Override
	public void send(Destination arg0, Message arg1) throws JMSException {
		// TODO Auto-generated method stub
	}

	@Override
	public void send(Message arg0, int arg1, int arg2, long arg3)
			throws JMSException {
		// TODO Auto-generated method stub
	}

	@Override
	public void send(Destination arg0, Message arg1, int arg2, int arg3,
			long arg4) throws JMSException {
		// TODO Auto-generated method stub
	}

	@Override
	public void setDeliveryMode(int deliveryMode) throws JMSException {
		this.deliveryMode.set(deliveryMode);
	}
	
	@Override
	public void setPriority(int priority) throws JMSException {
		this.priority.set(priority);
	}

	@Override
	public void setTimeToLive(long timeToLive) throws JMSException {
		this.timeToLive.set(timeToLive);
	}

	@Override
	public void close() throws JMSException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Destination getDestination() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getDisableMessageID() throws JMSException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean getDisableMessageTimestamp() throws JMSException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setDisableMessageID(boolean arg0) throws JMSException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDisableMessageTimestamp(boolean arg0) throws JMSException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Topic getTopic() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void publish(Message arg0) throws JMSException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void publish(Topic arg0, Message arg1) throws JMSException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void publish(Message arg0, int arg1, int arg2, long arg3)
			throws JMSException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void publish(Topic arg0, Message arg1, int arg2, int arg3, long arg4)
			throws JMSException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Queue getQueue() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void send(Queue arg0, Message arg1) throws JMSException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void send(Queue arg0, Message arg1, int arg2, int arg3, long arg4)
			throws JMSException {
		// TODO Auto-generated method stub
		
	}


}
