package com.sugarmq.core;

import java.io.Serializable;
import java.util.concurrent.ThreadPoolExecutor;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sugarmq.constant.MessageProperty;
import com.sugarmq.consumer.SugarMQMessageConsumer;
import com.sugarmq.message.SugarMQDestination;
import com.sugarmq.message.bean.SugarMQTextMessage;
import com.sugarmq.producer.SugarMQMessageProducer;
import com.sugarmq.transport.MessageDispatcher;
import com.sugarmq.util.SessionIdGenerate;

public class SugarMQSession implements Session{
	private String sessionId;
	private boolean transacted;	// 事务标记
	
	private Logger logger = LoggerFactory.getLogger(SugarMQSession.class);
	
	private MessageDispatcher messageDispatcher;
	
	// 消费者消费消息和发送应答消息的线程池执行器
	private ThreadPoolExecutor threadPoolExecutor;
	
	public SugarMQSession(String sessionId, boolean transacted, MessageDispatcher messageDispatcher) throws JMSException {
		this.sessionId = sessionId;
		this.transacted = transacted;
		this.messageDispatcher = messageDispatcher;
	}
	
	public void start() {
		threadPoolExecutor
	}
	
	@Override
	public void close() throws JMSException {
		// TODO Auto-generated method stub
	}

	@Override
	public void commit() throws JMSException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public QueueBrowser createBrowser(Queue arg0) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public QueueBrowser createBrowser(Queue arg0, String arg1)
			throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BytesMessage createBytesMessage() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageConsumer createConsumer(Destination destination) throws JMSException {
		if(!(destination instanceof SugarMQDestination)) {
			logger.warn("传入的Destination非法！");
			throw new JMSException("传入的Destination非法:" + destination);
		}
		
		SugarMQMessageConsumer sugarQueueReceiver = new SugarMQMessageConsumer(destination, 
				messageDispatcher.getSendMessageQueue(), 10);
		messageDispatcher.addConsumer(sugarQueueReceiver);
		return sugarQueueReceiver;
	}

	@Override
	public MessageConsumer createConsumer(Destination arg0, String arg1)
			throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageConsumer createConsumer(Destination arg0, String arg1,
			boolean arg2) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TopicSubscriber createDurableSubscriber(Topic arg0, String arg1)
			throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TopicSubscriber createDurableSubscriber(Topic arg0, String arg1,
			String arg2, boolean arg3) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MapMessage createMapMessage() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Message createMessage() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectMessage createObjectMessage() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectMessage createObjectMessage(Serializable arg0)
			throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageProducer createProducer(Destination destination) throws JMSException {
		if(!(destination instanceof SugarMQDestination)) {
			logger.warn("传入的Destination非法！");
			throw new JMSException("传入的Destination非法！");
		}
		
		SugarMQMessageProducer sugarQueueSender = new SugarMQMessageProducer(destination, messageDispatcher);
		return sugarQueueSender;
	}

	@Override
	public Queue createQueue(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StreamMessage createStreamMessage() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TemporaryQueue createTemporaryQueue() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TemporaryTopic createTemporaryTopic() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TextMessage createTextMessage() throws JMSException {
		SugarMQTextMessage sugarTextMessage = new SugarMQTextMessage();
		return sugarTextMessage;
	}

	@Override
	public TextMessage createTextMessage(String message) throws JMSException {
		SugarMQTextMessage sugarTextMessage = new SugarMQTextMessage(message);
		sugarTextMessage.setStringProperty(MessageProperty.SESSION_ID.getKey(), sessionId);
		return sugarTextMessage;
	}

	@Override
	public Topic createTopic(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getAcknowledgeMode() throws JMSException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public MessageListener getMessageListener() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getTransacted() throws JMSException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void recover() throws JMSException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollback() throws JMSException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setMessageListener(MessageListener arg0) throws JMSException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void unsubscribe(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		
	}
}
