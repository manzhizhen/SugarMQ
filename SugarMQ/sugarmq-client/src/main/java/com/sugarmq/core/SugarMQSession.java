package com.sugarmq.core;

import java.io.Serializable;

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
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sugarmq.constant.MessageProperty;
import com.sugarmq.message.bean.SugarTextMessage;
import com.sugarmq.queue.SugarQueue;
import com.sugarmq.queue.SugarQueueReceiver;
import com.sugarmq.queue.SugarQueueSender;
import com.sugarmq.transport.SugarMQTransport;
import com.sugarmq.util.SessionIdgenerate;

public class SugarMQSession implements Session, QueueSession, TopicSession{
	private String sessionId;
	private boolean transacted;	// 事务标记
	
	private Logger logger = LoggerFactory.getLogger(SugarMQSession.class);
	
	private SugarMQTransport sugarMQTransport;
	
	public SugarMQSession(boolean transacted, SugarMQTransport sugarMQTransport) throws JMSException {
		this.sessionId = SessionIdgenerate.getNewSessionId();
		this.transacted = transacted;
		this.sugarMQTransport = sugarMQTransport;
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
		if(!(destination instanceof SugarQueue)) {
			logger.warn("传入的Destination非法！");
			throw new JMSException("传入的Destination非法:" + destination);
		}
		
		SugarQueueReceiver sugarQueueReceiver = new SugarQueueReceiver(sugarMQTransport, destination);
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
		if(!(destination instanceof SugarQueue)) {
			logger.warn("传入的Destination非法！");
			throw new JMSException("传入的Destination非法！");
		}
		
		SugarQueueSender sugarQueueSender = new SugarQueueSender(destination, sugarMQTransport);
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
		SugarTextMessage sugarTextMessage = new SugarTextMessage();
		return sugarTextMessage;
	}

	@Override
	public TextMessage createTextMessage(String message) throws JMSException {
		SugarTextMessage sugarTextMessage = new SugarTextMessage(message);
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

	@Override
	public TopicPublisher createPublisher(Topic arg0) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TopicSubscriber createSubscriber(Topic arg0) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TopicSubscriber createSubscriber(Topic arg0, String arg1,
			boolean arg2) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public QueueReceiver createReceiver(Queue arg0) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public QueueReceiver createReceiver(Queue arg0, String arg1)
			throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public QueueSender createSender(Queue arg0) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}
}
