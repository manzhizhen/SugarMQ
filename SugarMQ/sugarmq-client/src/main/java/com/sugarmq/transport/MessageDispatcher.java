/**
 * 
 */
package com.sugarmq.transport;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sugarmq.constant.MessageProperty;
import com.sugarmq.constant.MessageType;
import com.sugarmq.consumer.SugarMQMessageConsumer;
import com.sugarmq.message.bean.SugarMQMessage;
import com.sugarmq.util.DateUtils;
import com.sugarmq.util.MessageIdGenerate;

/**
 * 消息分发和消息发送的实现类
 * @author manzhizhen
 *
 */
public class MessageDispatcher {
	// key-消费者ID，value-消费者对象
	private ConcurrentMap<String, MessageConsumer> consumerMap = new ConcurrentHashMap<String, MessageConsumer>();
//	private int nextIndex = 0;	// 下一个消费者索引
	
	// 生产者发送消息的应答Map,key-发送的消息ID，value-消息应答成功的闭锁
	private ConcurrentMap<String, CountDownLatch> producerAckMap = new ConcurrentHashMap<String, CountDownLatch>();
	// 消费者注册Map，key-客户端设定的消费者ID,value-消息应答成功的闭锁
	private ConcurrentMap<String, DataCountDownLatch<Message>> addConsumerAckMap = new ConcurrentHashMap<String, DataCountDownLatch<Message>>();
	
	private BlockingQueue<Message> receiveMessageQueue; // 待分发的消息队列
	private BlockingQueue<Message> sendMessageQueue;	// 发送消息的消息队列
	
	private Thread dispatcherThread;
	private ThreadPoolExecutor threadPoolExecutor;
	
	private Logger logger = LoggerFactory.getLogger(MessageDispatcher.class);
	
	/**
	 * @param dispatchType
	 * @param acknowledgeType
	 */
	public MessageDispatcher(BlockingQueue<Message> receiveMessageQueue, BlockingQueue<Message> sendMessageQueue) {
		this.receiveMessageQueue = receiveMessageQueue;
		this.sendMessageQueue = sendMessageQueue;
	}
	
	/**
	 * 开始工作
	 */
	public void start() {
		logger.info("MessageDispatcher准备开始工作... ...");
		
		threadPoolExecutor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		
		dispatcherThread = new Thread(new Runnable() {
			@Override
			public void run() {
				Message message = null;
				while(!Thread.currentThread().isInterrupted()) {
					try {
						message = receiveMessageQueue.take();
					} catch (InterruptedException e) {
						logger.info("MessageDispatcher被中断，即将退出.");
						break ;
					}
					
					logger.debug("开始处理消息【{}】", message);
					
					try {
						// 生产者应答消息
						if(MessageType.PRODUCER_ACKNOWLEDGE_MESSAGE.getValue().
								equals(message.getJMSType())) {
							logger.debug("客户端接收到服务端的生产者应答消息:{}", message);
							CountDownLatch countDownLatch = producerAckMap.get(message.getJMSMessageID());
							if(countDownLatch != null) {
								countDownLatch.countDown();
								producerAckMap.remove(message.getJMSMessageID());
							}
						
						// 要分配给消费者的消息
						} else if(MessageType.PRODUCER_MESSAGE.getValue().
								equals(message.getJMSType())) {
							logger.debug("客户端接收到要分配给消费者的消息:{}", message);
							String customerId = message.getStringProperty(MessageProperty.CUSTOMER_ID.getKey());
							if(StringUtils.isBlank(customerId)) {
								logger.error("客户端接收到生产者发来的消息中消费者ID为空，分配消息到消费者失败:{}", message);
								continue ;
							}
						
							MessageConsumer consumer = consumerMap.get(customerId);
							
							ConsumeMessageTask task = new ConsumeMessageTask(consumer, message);
							threadPoolExecutor.execute(task);
							
						// 消费者注册应答消息	
						} else if(MessageType.CUSTOMER_REGISTER_ACKNOWLEDGE_MESSAGE.getValue().
								equals(message.getJMSType())) {
							logger.debug("客户端接收到服务器发来的发来消费者注册应答消息:{}", message);
							String customerId = message.getStringProperty(MessageProperty.CUSTOMER_ID.getKey());
							String customerClientId = message.getStringProperty(MessageProperty.CUSTOMER_CLIENT_ID.getKey());
						
							if(StringUtils.isBlank(customerId) || StringUtils.isBlank(customerClientId)) {
								logger.error("消费者注册应答消息数据不完整，处理失败【{}】", message);
							}
							
							DataCountDownLatch<Message> countDownLatch = addConsumerAckMap.get(customerClientId);
							countDownLatch.setData(message);
							countDownLatch.countDown();
						} else {
							logger.error("未知消息类型，无法处理【{}】", message);
						}
						
					} catch (JMSException e) {
						logger.error("SugarMQQueueDispatcher消息处理失败:【{}】", message, e);
					}
				}
			}
		});
		
		dispatcherThread.start();
		logger.info("MessageDispatcher已经开始工作");
	}
	
	/**
	 * 注册一个消费者
	 * 消费者ID可以由客户端设置，最终是否采纳由服务器决定
	 * @param messageConsumer
	 */
	public synchronized void addConsumer(MessageConsumer messageConsumer) throws JMSException {
		logger.info("准备注册一个消费者：{}", messageConsumer);
		
		if(messageConsumer == null || !(messageConsumer instanceof SugarMQMessageConsumer)) {
			throw new IllegalArgumentException("MessageConsumer为空或类型错误！");
		}
		
		SugarMQMessageConsumer consumer = (SugarMQMessageConsumer) messageConsumer;
		String consumerId = consumer.getConsumerId();
		if(StringUtils.isBlank(consumerId) || addConsumerAckMap.containsKey(consumerId)) {
			consumerId = getNewCustomerId();
		}
		Message addConsumerMsg = new SugarMQMessage();
		addConsumerMsg.setStringProperty(MessageProperty.CUSTOMER_CLIENT_ID.getKey(), consumerId);
		addConsumerMsg.setJMSType(MessageType.CUSTOMER_REGISTER_MESSAGE.getValue());
		addConsumerMsg.setJMSDestination(consumer.getDestination());
		
		DataCountDownLatch<Message> dataCountDownLatch = new DataCountDownLatch<Message>(1);
		addConsumerAckMap.put(consumerId, dataCountDownLatch);
		try {
			sendMessageQueue.put(addConsumerMsg);
			dataCountDownLatch.await();
		} catch (InterruptedException e) {
			logger.info("靠，有必要吗？注册个消费者你也要中断？");
		}
		
		Message ackMsg = dataCountDownLatch.getData();
		
		consumerId = ackMsg.getStringProperty(MessageProperty.CUSTOMER_ID.getKey());
		consumer.setConsumerId(consumerId);
		consumerMap.put(consumerId, consumer);
		
		addConsumerAckMap.remove(ackMsg.getStringProperty(MessageProperty.CUSTOMER_ID.getKey()));
		
		logger.info("成功注册了一个消费者：{}", messageConsumer);
	}
	
	/**
	 * 同步发送生产者的消息
	 * @param message
	 * @throws JMSException
	 */
	public void sendMessage(Message message) throws JMSException {
		String messageId = MessageIdGenerate.getNewMessageId();
		message.setJMSMessageID(messageId);
		CountDownLatch countDownLatch = new CountDownLatch(1);
		producerAckMap.put(messageId, countDownLatch);
		
		try {
			sendMessageQueue.put(message);
			countDownLatch.await();
		} catch (InterruptedException e) {
			logger.error("SugarMQMessageProducer消息发送被中断！");
			throw new JMSException("SugarMQMessageProducer消息发送被中断:" + e.getMessage());
		}
		
	}
	
	/**
	 * 生成一个消费者ID
	 * 非线程安全
	 * @return
	 */
	private String getNewCustomerId() {
		String newId = DateUtils.formatDate(DateUtils.DATE_FORMAT_TYPE2);
		Random random = new Random(new Date().getTime());
		int next = random.nextInt(1000000);
		while(true) {
			if(addConsumerAckMap.containsKey(newId + next)) {
				next = random.nextInt(1000000);
			} else {
				break ;
			}
		}
		
		logger.debug("生成的消费者ID为【{}】", newId + next);
		return newId + next;
	}
	
	/**
	 * 类说明：消息异步消费和应答
	 *
	 * 类描述：
	 * @author manzhizhen
	 *
	 * 2014年12月17日
	 */
	class ConsumeMessageTask implements Runnable {
		private MessageConsumer consumer;
		private Message message;
		
		private Logger logger = LoggerFactory.getLogger(ConsumeMessageTask.class);
		
		public ConsumeMessageTask(MessageConsumer consumer, Message message) {
			this.consumer = consumer;
			this.message = message;
		}

		@Override
		public void run() {
			try {
				MessageListener listener = consumer.getMessageListener();
				if(listener == null) {
					logger.error("消费者{}没有配置消息监听器！", consumer);
					return ;
				}
				
				listener.onMessage(message);
				
			} catch (JMSException e) {
				logger.error("消费者{}消费消息失败！", e);
			}
			
		}
	}
	
	/**
	 * 类说明：带有消息负载的闭锁
	 *
	 * 类描述：主要用于阻塞发送后返回应答消息
	 * @author manzhizhen
	 *
	 * 2014年12月17日
	 */
	class DataCountDownLatch<T> extends CountDownLatch {
		private T data;
		
		public DataCountDownLatch(int count) {
			super(count);
		}

		public T getData() {
			return data;
		}

		public void setData(T data) {
			this.data = data;
		}
	}
}
