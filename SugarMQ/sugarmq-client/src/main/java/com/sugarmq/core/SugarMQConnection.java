package com.sugarmq.core;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sugarmq.constant.ConnectionProperty;
import com.sugarmq.constant.MessageType;
import com.sugarmq.message.bean.SugarMQMapMessage;
import com.sugarmq.transport.MessageDispatcher;
import com.sugarmq.transport.SugarMQTransport;

/**
 * 
 * @author manzhizhen
 *
 */
public class SugarMQConnection implements Connection{
	
	private SugarMQTransport sugarMQTransport;
	
	private MessageDispatcher messageDispatcher;
	
	private AtomicBoolean isStarted = new AtomicBoolean(false);
	private AtomicBoolean isClosed = new AtomicBoolean(false);
	
	// 连接参数Map
	private ConcurrentMap<String, Object> params = new ConcurrentHashMap<String, Object>();
	
	private Logger logger = LoggerFactory.getLogger(SugarMQConnection.class);
	
	public SugarMQConnection(SugarMQTransport sugarMQTransport) {
		if(sugarMQTransport == null) {
			throw new IllegalArgumentException("SugarMQTransport不能为空！");
		}
		
		this.sugarMQTransport = sugarMQTransport;
		messageDispatcher = new MessageDispatcher(sugarMQTransport.getReceiveMessageQueue(), 
				sugarMQTransport.getSendMessageQueue());
		
	}

	@Override
	public void close() throws JMSException {
		synchronized (isClosed) {
			if(isClosed.get()) {
				return ;
			}
			
			isClosed.set(true);
		}

		logger.info("SugarMQConnection即将关闭... ...");
		
		messageDispatcher.close();
		sugarMQTransport.close();
		
		logger.info("SugarMQConnection已经关闭！");
	}

	@Override
	public ConnectionConsumer createConnectionConsumer(Destination destination,
			String arg1, ServerSessionPool arg2, int arg3) throws JMSException {
		return null;
	}

	@Override
	public ConnectionConsumer createDurableConnectionConsumer(Topic arg0,
			String arg1, String arg2, ServerSessionPool arg3, int arg4)
			throws JMSException {
		return null;
	}

	@Override
	public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
		sugarMQTransport.setAcknowledgeType(acknowledgeMode);
		SugarMQSession sugarMQSession = new SugarMQSession(transacted, messageDispatcher);
		return sugarMQSession;
	}

	@Override
	public String getClientID() throws JMSException {
		return null;
	}

	@Override
	public ExceptionListener getExceptionListener() throws JMSException {
		return null;
	}

	@Override
	public ConnectionMetaData getMetaData() throws JMSException {
		return null;
	}

	@Override
	public void setClientID(String arg0) throws JMSException {
	}

	@Override
	public void setExceptionListener(ExceptionListener arg0)
			throws JMSException {
	}

	@Override
	public void start() throws JMSException {
		synchronized (isStarted) {
			if(isStarted.get()) {
				return ;
			}
			
			logger.info("SugarMQConnection开始启动！");
			sugarMQTransport.start();
			messageDispatcher.start();
			
			// 将客户端的定制参数传给服务端
			SugarMQMapMessage message = new SugarMQMapMessage();
			message.setJMSType(MessageType.CUSTOMER_MESSAGE_PULL.getValue());
			message.setInt(ConnectionProperty.CLIENT_MESSAGE_BATCH_ACK_QUANTITY.getKey(), 
					(int) ConnectionProperty.CLIENT_MESSAGE_BATCH_ACK_QUANTITY.getValue());
			
			messageDispatcher.sendMessage(message);
			
			isStarted.set(true);
		}
	}

	@Override
	public void stop() throws JMSException {
		sugarMQTransport.close();
	}

	public Object getParamValue(String key) {
		return params.get(key);
	}

	public void setParamsValue(String key, Object value) {
		synchronized (isStarted) {
			if(isStarted.get()) {
				throw new IllegalStateException("SugarMQConnection已经开启，无法设置customerBatchAckNum！");
			}
			
			params.put(key, value);
		}
	}
}
