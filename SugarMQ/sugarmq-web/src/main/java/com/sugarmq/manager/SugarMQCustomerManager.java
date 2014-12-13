/**
 * 
 */
package com.sugarmq.manager;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.sugarmq.constant.MessageProperty;
import com.sugarmq.constant.MessageType;
import com.sugarmq.queue.SugarMQMessageContainer;
import com.sugarmq.util.DateUtils;

/**
 * 类说明：消费者管理器
 *
 * 类描述：
 * @author manzhizhen
 *
 * 2014年12月12日
 */
@Component
public class SugarMQCustomerManager {
	// key-客户端消费者ID, value-SugarMQServerTransport的sendMessageQueue
	private ConcurrentHashMap<String, BlockingQueue<Message>> customerMap = 
			new ConcurrentHashMap<String, BlockingQueue<Message>>();
	
	// key-目的地名称，value-消费者ID容器
	private ConcurrentHashMap<String,  ErgodicArray<String>> destinationMap = 
			new ConcurrentHashMap<String,  ErgodicArray<String>>();
	
	private Logger logger = LoggerFactory.getLogger(SugarMQCustomerManager.class);
	
	/**
	 * 新注册一个消费者
	 * @param message
	 * @throws JMSException 
	 */
	public void addCustomer(Message message, BlockingQueue<Message> sendMessageQueue) throws JMSException {
		if(message == null || !MessageType.CUSTOMER_REGISTER_MESSAGE.getValue().
				equals(message.getStringProperty(MessageProperty.MESSAGE_TYPE.getKey()))
				|| sendMessageQueue == null) {
			throw new IllegalArgumentException();
		}
		
		String customerId = message.getStringProperty(MessageProperty.CUSTOMER_ID.getKey());
		if(StringUtils.isBlank(customerId) || customerMap.containsKey(customerId)) {
			logger.debug("客户端没有填写消费者ID【{}】", message);
			customerId = getNewCustomerId();
		}
		
		customerMap.put(customerId, sendMessageQueue);
		ErgodicArray<String> ergodicArray = destinationMap.putIfAbsent(((SugarMQMessageContainer)message.
				getJMSDestination()).getQueueName(), new ErgodicArray<String>());
		ergodicArray.add(customerId);
	}
	
	/**
	 * 将消息推送到一个消费者的待发送队列中
	 * @throws JMSException 
	 */
	public void putMessageToCustomerQueue(Message message) throws JMSException {
		if(message == null) {
			throw new IllegalArgumentException("Message不能为空！");
		}
		
		SugarMQMessageContainer sugarMQMessageContainer = (SugarMQMessageContainer) message.getJMSDestination();
		ErgodicArray<String> ergodicArray = destinationMap.putIfAbsent(sugarMQMessageContainer.getQueueName(), new ErgodicArray<String>());
		
		String nextCustomerId = ergodicArray.getNext();
		
		BlockingQueue<Message> queue = customerMap.get(nextCustomerId);
		
		try {
			queue.put(message);
			logger.debug("成功将消息【{}】推送到消费者【{}】队列！", message, nextCustomerId);
		} catch (InterruptedException e) {
			logger.error("将消息【{}】推送到消费者【{}】队列失败！", message, nextCustomerId);
		}
	}
	
	/**
	 * 生成一个消费者ID
	 * 非线程安全，后期需要改成线程安全
	 * @return
	 */
	private String getNewCustomerId() {
		String newId = DateUtils.formatDate(DateUtils.DATE_FORMAT_TYPE2);
		Random random = new Random(new Date().getTime());
		int next = random.nextInt(1000000);
		while(true) {
			if(customerMap.containsKey(newId + next)) {
				next = random.nextInt(1000000);
			} else {
				break ;
			}
		}
		
		logger.debug("生成的消费者ID为【{}】", newId + next);
		return newId + next;
	}
	
	/**
	 * 类说明：可按顺序遍历的数组结构
	 *
	 * 类描述:线程安全
	 * @author manzhizhen
	 *
	 * 2014年12月12日
	 */
	class ErgodicArray<T> {
		private CopyOnWriteArrayList<T> contentArray = new CopyOnWriteArrayList<T>();
		private int index = 0;
		private ReentrantLock lock = new ReentrantLock();
		
		public T getNext() {
			lock.lock();
			while(!contentArray.isEmpty());
			
			lock.lock();
			if(index >= contentArray.size()) {
				index = 0;
			}
			
			T t = contentArray.get(index);
			index++;
			if(index >= contentArray.size()) {
				index = 0;
			}
			
			lock.unlock();
			
			return t;
		}
		
		public void add(T t) {
			contentArray.addIfAbsent(t);
		}
		
		public void remove(T t) {
			lock.lock();
			contentArray.remove(t);
			lock.unlock();
		}
		
		public boolean isEmpty() {
			return contentArray.isEmpty();
		}
	}
}



