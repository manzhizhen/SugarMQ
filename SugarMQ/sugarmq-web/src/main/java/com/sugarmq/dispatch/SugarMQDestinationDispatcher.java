/**
 * 
 */
package com.sugarmq.dispatch;

import java.util.concurrent.BlockingQueue;

import javax.jms.JMSException;
import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sugarmq.constant.MessageProperty;
import com.sugarmq.constant.MessageType;
import com.sugarmq.manager.SugarMQCustomerManager;
import com.sugarmq.manager.SugarMQMessageManager;
import com.sugarmq.message.bean.SugarMQBytesMessage;

/**
 * 消息目的地分发器
 * 每个Transprot连接器配置一个该分发器来分发消息到目的地
 * @author manzhizhen
 *
 */
public class SugarMQDestinationDispatcher {
	private BlockingQueue<Message> receiveMessageQueue;
	private BlockingQueue<Message> sendMessageQueue;
	private SugarMQMessageManager sugarMQMessageManager;
	private SugarMQCustomerManager sugarMQCustomerManager;
	
	private static Logger logger = LoggerFactory.getLogger(SugarMQDestinationDispatcher.class);
	
	public SugarMQDestinationDispatcher(BlockingQueue<Message> receiveMessageQueue, 
			BlockingQueue<Message> sendMessageQueue, SugarMQMessageManager sugarMQMessageManager, 
			SugarMQCustomerManager sugarMQCustomerManager) {
		if(receiveMessageQueue == null || sendMessageQueue == null || sugarMQMessageManager == null
				|| sugarMQCustomerManager == null) {
			throw new IllegalArgumentException();
		}
		
		this.receiveMessageQueue = receiveMessageQueue;
		this.sendMessageQueue = sendMessageQueue;
		this.sugarMQMessageManager = sugarMQMessageManager;
		this.sugarMQCustomerManager = sugarMQCustomerManager;
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
						sendMessageQueue.put(answerMessage);
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
					sugarMQCustomerManager.addCustomer(message, sendMessageQueue);
				
				// 消费者拉取消息
				} else if(MessageType.CUSTOMER_MESSAGE_PULL.getValue().
						equals(message.getStringProperty(MessageProperty.MESSAGE_TYPE.getKey()))) {
					// TODO:
					
				} else {
					logger.error("未知消息类型，无法处理【{}】", message);
				}
				
			} catch (JMSException e) {
				logger.error("SugarMQQueueDispatcher消息处理失败:【{}】", message, e);
			}
		}
	}
}
