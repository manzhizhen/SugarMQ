/**
 * 
 */
package com.sugarmq.manager;

import java.util.Date;
import java.util.Map;
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
import com.sugarmq.dispatch.SugarMQConsumerDispatcher;
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
public class SugarMQConsumerManager {
	// key-客户端消费者ID, value-SugarMQServerTransport的sendMessageQueue
	private ConcurrentHashMap<String, BlockingQueue<Message>> customerMap = 
			new ConcurrentHashMap<String, BlockingQueue<Message>>();
	
	// key-目的地名称，value-消费者ID容器
	private ConcurrentHashMap<String,  ErgodicArray<String>> destinationMap = 
			new ConcurrentHashMap<String,  ErgodicArray<String>>();
	
	// 消息分发器
	private ConcurrentHashMap<String, SugarMQConsumerDispatcher> consumerDispatcherMap = 
			new ConcurrentHashMap<String, SugarMQConsumerDispatcher>();
	
	private Logger logger = LoggerFactory.getLogger(SugarMQConsumerManager.class);
	
	/**
	 * 新注册一个消费者
	 * 只有新注册消费者才会触发新建消费者分发器SugarMQConsumerDispatcher对象
	 * @param message
	 * @throws JMSException 
	 */
	public void addCustomer(Message message, BlockingQueue<Message> sendMessageQueue) throws JMSException {
		if(message == null || !MessageType.CUSTOMER_REGISTER_MESSAGE.getValue().
				equals(message.getJMSType())
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
		
		if(ergodicArray == null) {
			ergodicArray = destinationMap.get(((SugarMQMessageContainer)message.
				getJMSDestination()).getQueueName());
		}
		
		ergodicArray.add(customerId);
		
		SugarMQMessageContainer container = (SugarMQMessageContainer) message.getJMSDestination();
		SugarMQConsumerDispatcher sugarMQConsumerDispatcher = consumerDispatcherMap.putIfAbsent(container.getName(), 
				new SugarMQConsumerDispatcher(this, container));
		
		if(sugarMQConsumerDispatcher == null) {
			sugarMQConsumerDispatcher = consumerDispatcherMap.get(container.getName());
		}
		
		if(!sugarMQConsumerDispatcher.isStart()) {
			sugarMQConsumerDispatcher.start();
		}
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
		message.setStringProperty(MessageProperty.CUSTOMER_ID.getKey(), nextCustomerId);
		try {
			queue.put(message);
			destinationMap.get(sugarMQMessageContainer.getQueueName()).setValue(nextCustomerId, false);
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
		// Boolean表示该消费者是否已经准备好接收消息
		private CopyOnWriteArrayList<Entry<T, Boolean>> contentArray = new CopyOnWriteArrayList<Entry<T, Boolean>>();
		private int index = 0;
		private ReentrantLock lock = new ReentrantLock();
		
		public T getNext() {
			lock.lock();
			
			Entry<T, Boolean> entry = null;
			while(true) {
				
				if(contentArray.isEmpty()) {
					continue ;
				}
				
				if(index >= contentArray.size()) {
					index = 0;
				}
				
				entry = (Entry<T, Boolean>) contentArray.get(index);
				index++;
				if(entry.getValue()) {
					break ;
					
				}
			}
			
			lock.unlock();
			
			return entry.getKey();
		}
		
		public void add(T t) {
			contentArray.addIfAbsent(new Entry<T, Boolean>(t, new Boolean(true)));
		}
		
		public void remove(T t) {
			lock.lock();
			contentArray.remove(t);
			lock.unlock();
		}
		
		public boolean isEmpty() {
			return contentArray.isEmpty();
		}
		
		public void setValue(T t, Boolean isIdle) {
			for(Entry<T, Boolean> entry : contentArray) {
				if(entry.getKey().equals(t)) {
					entry.setValue(isIdle);
					break ;
				}
			}
		}
		
		private class Entry<K,V> implements Map.Entry<K, V> {
			private K key;
			private V value;
			
			public Entry(K key, V value) {
				this.key = key;
				this.value = value;
			}
			
			@Override
			public K getKey() {
				return key;
			}
			
			@Override
			public V getValue() {
				return value;
			}
			
			@Override
			public V setValue(V value) {
				this.value = value;
				return this.value;
			}
		}
	}
}



