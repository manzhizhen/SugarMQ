package com.sugarmq.manager;

import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.sugarmq.constant.MessageProperty;
import com.sugarmq.manager.tcp.TcpMessageDispatcher;
import com.sugarmq.message.bean.SugarMessage;
import com.sugarmq.queue.SugarMQQueue;
import com.sugarmq.queue.SugarQueue;
import com.sugarmq.util.MessageIdGenerate;

/**
 * 对MOM的队列进行管理
 * 
 * @author Manzhizhen
 * 
 */
@Component
public class SugarMQQueueManager {
	private @Value("${max_queue_message_num}")int MAX_QUEUE_MESSAGE_CAPACITY; // 队列中所能容纳的消息最大数
	private @Value("${max_queue_num}")int MAX_QUEUE_NUM; // 队列数量的最大值

	// 消息队列
	private Map<String, SugarMQQueue> queueMap;
	// 消息队列分发线程
	private Map<String, TcpMessageDispatcher> queueDispatchThreadMap;
//	// 消息队列的发送端连接
//	private Map<String, ConcurrentLinkedQueue<SocketChannel>> queueSenderMap;
	// 消息队列的接收端连接
//	private Map<String, ConcurrentLinkedQueue<SocketChannel>> queueReceiverMap;
	
	private Logger logger = LoggerFactory.getLogger(SugarMQQueueManager.class);
	
	public void init() {
		queueMap = new ConcurrentHashMap<String, SugarMQQueue>();
		queueDispatchThreadMap = new ConcurrentHashMap<String, TcpMessageDispatcher>();
//		queueSenderMap = new ConcurrentHashMap<String, ConcurrentLinkedQueue<SocketChannel>>(10);
//		queueReceiverMap = new ConcurrentHashMap<String, ConcurrentLinkedQueue<SocketChannel>>();
	}
	
	/**
	 * 新增一个队列 如果队列已经存在，则忽略
	 * @param queueName
	 */
	private SugarMQQueue addQueue(SugarQueue sugarQueue) throws JMSException{
		String queueName = sugarQueue.getQueueName();
		if (StringUtils.isBlank(queueName)) {
			logger.warn("队列名称不能为空！");
			throw new JMSException("队列名称不能为空！");
		}
		
		SugarMQQueue queue = null;
		synchronized (queueMap) {
			if (!queueMap.containsKey(queueName)) {
				queue = new SugarMQQueue(queueName);
				// 创建分发该队列的线程
				TcpMessageDispatcher tcpDispatchThread = new TcpMessageDispatcher(queue);
				queueMap.put(queueName, queue);
				queueDispatchThreadMap.put(queueName, tcpDispatchThread);
				new Thread(tcpDispatchThread).start();
			}
			
			if (queueMap.size() >= MAX_QUEUE_NUM) {
				logger.warn("MOM中队列数已满，添加队列失败:{}", queueName);
				throw new JMSException("MOM中队列数已满，添加队列失败:{}", queueName);
			}
		}
		
		return queue;
	}
	
	/**
	 * 将一个消息放入队列中
	 * @param message
	 */
	private void addMessage(Message message) throws JMSException{
		// 如果是持久化消息，需要将消息持久化。
		if(DeliveryMode.PERSISTENT == message.getJMSDeliveryMode()) {
			logger.info("持久化消息:{}", message);
			persistentMessage(message);
		}
		
		// 将消息放入消息队列
		SugarQueue sugarQueue = (SugarQueue) message.getJMSDestination();
		SugarMQQueue queue = queueMap.get(sugarQueue.getQueueName());
		if(queue == null) {
			logger.info("新增队列:{}", sugarQueue);
			queue = addQueue(sugarQueue);
		}
		
		logger.debug("将消息放入分发队列:{}", message);
		queue.putMessage(message);
	}
	
	/**
	 * 将一个消息从消息队列中移除
	 * @param message
	 */
	private void removeMessage(Message message) throws JMSException{
		// 将消息放入消息队列
		SugarQueue sugarQueue = (SugarQueue) message.getJMSDestination();
		SugarMQQueue queue = queueMap.get(sugarQueue.getQueueName());
		
		if(queue != null) {
			queue.removeMessage(message);
		} else {
			logger.error("不存在的队列名称【{}】，移除消息{}失败！", sugarQueue.getQueueName(), message);
		}
		
		// 如果是持久化消息，需要将消息持久化。
		if(DeliveryMode.PERSISTENT == message.getJMSDeliveryMode()) {
			removePersistentMessage(message);
		}
	}
	
	/**
	 * 持久化一条消息
	 * @param message
	 * @throws JMSException
	 */
	private void persistentMessage(Message message) throws JMSException{
		//TODO
	}
	
	/**
	 * 将消息从持久化中移除
	 * @param message
	 * @throws JMSException
	 */
	private void removePersistentMessage(Message message) throws JMSException{
		//TODO
	}
	
	/**
	 * 从生产者那里获取消息
	 * @param message
	 * @throws JMSException
	 */
	public Message receiveProducerMessage(Message message) throws JMSException{
//		if(!(message instanceof SugarMessage)) {
//			logger.error("接收到的生产者Message类型非法：" + message);
//			throw new JMSException("接收到的Message类型非法：" + message);
//		}
		
		// 获取客户端给消息设置的MessageId
		String clientMessageId = message.getJMSMessageID();
		
		if(!message.getBooleanProperty(MessageProperty.DISABLE_MESSAGE_ID.getKey())) {
			message.setJMSMessageID(MessageIdGenerate.getNewMessageId());
		} else {
			message.setJMSMessageID(null);
		}
		
		// 添加消息
		addMessage(message);
		
		Message acknowledgeMessage = new SugarMessage();
		acknowledgeMessage.setJMSMessageID(clientMessageId);
		
		return acknowledgeMessage;
	}
	
	/**
	 * 从消费者那里接受消息应答
	 * @param message
	 * @throws JMSException
	 */
	public void receiveConsumerAcknowledgeMessage(Message message) throws JMSException {
		removeMessage(message);
	}
	
	
}
