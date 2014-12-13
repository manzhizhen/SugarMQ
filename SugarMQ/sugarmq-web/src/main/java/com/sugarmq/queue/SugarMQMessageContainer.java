/**
 * 
 */
package com.sugarmq.queue;

import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Topic;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sugarmq.constant.MessageContainerType;

/**
 * 队列和主题的消息容器
 * 
 * @author manzhizhen
 *
 */
public class SugarMQMessageContainer implements Queue, Topic{
	private String name;
	private String type;
	
	// 待发送消息队列
	private LinkedBlockingQueue<Message> messageQueue = new LinkedBlockingQueue<Message>();
	// 已发送的消息队列
	private LinkedBlockingQueue<Message> consumeMessageQueue = new LinkedBlockingQueue<Message>();
	
	private static Logger logger = LoggerFactory.getLogger(SugarMQMessageContainer.class);
	
	public SugarMQMessageContainer(String name, String type) {
		if(StringUtils.isBlank(name) || StringUtils.isBlank(type)) {
			throw new IllegalArgumentException();
		}
		
		this.name = name;
		this.type = type;
	}
	
	@Override
	public String getTopicName() throws JMSException {
		return name;
	}
	
	@Override
	public String getQueueName() throws JMSException {
		return name;
	}
	
	public boolean isQueue() {
		return MessageContainerType.QUEUE.getValue().equals(type);
	}
	
	public boolean isTopic() {
		return MessageContainerType.TOPIC.getValue().equals(type);
	}
	
	/**
	 * 往消息队列中放入一条消息
	 * @throws JMSException 
	 */
	public void putMessage(Message message) throws JMSException {
		try {
			message.setJMSTimestamp(new Date().getTime());
			messageQueue.put(message);
			logger.debug("往队列【{}】添加一条消息:{}", name, message);
		} catch (InterruptedException e) {
			logger.error("往队列【{}】添加消息【{}】失败:{}", name, message, e);
			throw new JMSException(e.getMessage());
		}
	}
	
	/**
	 * 从队列中获取一个消息
	 * 没消息则阻塞
	 * @return
	 * @throws JMSException 
	 */
	public Message takeMessage() throws JMSException {
		Message message = null;
		 try {
			message = messageQueue.take();
			logger.debug("从队列【{}】取出一条消息:{}", name, message);
		} catch (InterruptedException e) {
			logger.error("从队列【{}】获取息失败:{}", name, e);
			throw new JMSException(e.getMessage());
		}
		 
		return message;
	}
	
	/**
	 * 移除一条消息
	 * @param message
	 */
	public void removeMessage(Message message) {
		consumeMessageQueue.remove(message);
	}
}
