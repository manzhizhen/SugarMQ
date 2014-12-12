/**
 * 
 */
package com.sugarmq.queue;

import java.net.Socket;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.JMSException;
import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sugarmq.constant.MessageProperty;
import com.sugarmq.constant.MessageType;
import com.sugarmq.manager.SugarMQMessageManager;
import com.sugarmq.message.bean.SugarMQBytesMessage;

/**
 * 分发消息线程
 * 每个队列有一个该线程负责分发消息
 * @author manzhizhen
 *
 */
public class SugarMQDestinationDispatcher {
	private BlockingQueue<Message> receiveMessageQueue;
	private BlockingQueue<Message> answerMessageQueue;
	private SugarMQMessageManager sugarMQMessageManager;
	
	private static Logger logger = LoggerFactory.getLogger(SugarMQDestinationDispatcher.class);
	
	public SugarMQDestinationDispatcher(BlockingQueue<Message> receiveMessageQueue, 
			BlockingQueue<Message> answerMessageQueue, SugarMQMessageManager sugarMQMessageManager) {
		if(receiveMessageQueue == null || answerMessageQueue == null || sugarMQMessageManager == null) {
			throw new IllegalArgumentException();
		}
		
		this.receiveMessageQueue = receiveMessageQueue;
		this.answerMessageQueue = answerMessageQueue;
		this.sugarMQMessageManager = sugarMQMessageManager;
	}
	
	public void start() {
		logger.info("SugarMQQueueDispatcher已经开始工作.");
		Message message = null;
		while(!Thread.currentThread().isInterrupted()) {
			try {
				message = receiveMessageQueue.take();
			} catch (InterruptedException e) {
				logger.info("SugarMQQueueDispatcher被中断，即将退出.");
				break ;
			}
			
			logger.debug("开始处理消息【{}】", message);
			
			try {
				// 生产者消息
				if(MessageType.PRODUCER_MESSAGE.getValue().
						equals(message.getStringProperty(MessageProperty.MESSAGE_TYPE.getKey()))) {
					sugarMQMessageManager.addMessage(message);
					// 创建应答消息
					SugarMQBytesMessage answerMessage = new SugarMQBytesMessage();
					answerMessage.setStringProperty(MessageProperty.MESSAGE_TYPE.getKey(), 
							MessageType.PRODUCER_ACKNOWLEDGE_MESSAGE.getValue());
					answerMessage.setJMSMessageID(message.getJMSMessageID());
					try {
						answerMessageQueue.put(answerMessage);
					} catch (InterruptedException e) {
						logger.info("SugarMQQueueDispatcher被中断，即将退出.");
					}
				
				// 消费者应答消息
				} else if(MessageType.CUSTOMER_ACKNOWLEDGE_MESSAGE.getValue().
						equals(message.getStringProperty(MessageProperty.MESSAGE_TYPE.getKey()))) {
					sugarMQMessageManager.removeMessage(message);
				
				// 消费者注册消息
				} else if(MessageType.CUSTOMER_REGISTER_MESSAGE.getValue().
						equals(message.getStringProperty(MessageProperty.MESSAGE_TYPE.getKey()))) {
					sugarMQMessageManager.removeMessage(message);
					
				} else {
					logger.error("未知消息类型，无法处理【{}】", message);
				}
				
			} catch (JMSException e) {
				logger.error("SugarMQQueueDispatcher消息处理失败:【{}】", message, e);
			}
		}
	}
}
