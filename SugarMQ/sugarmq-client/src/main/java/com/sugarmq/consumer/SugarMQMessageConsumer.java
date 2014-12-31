package com.sugarmq.consumer;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sugarmq.constant.ConsumerState;

public class SugarMQMessageConsumer implements MessageConsumer {
	private String consumerId;	// 消费者ID，可以由客户端设置，但最终由服务端来决定
	
	private String state;	// 消费者的状态
	
	private String messageSelector;
	private Destination destination;
	
	private Runnable run;
	
	// 未消费的消息队列
	private BlockingQueue<Message> messageQueue;
	
	private MessageListener messageListener;
	
	private Logger logger = LoggerFactory.getLogger(SugarMQMessageConsumer.class);
	
	public SugarMQMessageConsumer(Destination destination, int cacheSize){
		if(destination == null) {
			throw new IllegalArgumentException("创建消费者失败，Destination为空！");
		}
		
		if(cacheSize <= 0) {
			throw new IllegalArgumentException("创建消费者失败，cacheSize必须大于0！");
		}
		
		this.destination = destination;
		
		messageQueue = new LinkedBlockingQueue<Message>(cacheSize);
		
		run = new ConsumeMessageTask(this);
		
		// 设置状态为创建状态
		state = ConsumerState.CREATE.getValue();
		logger.debug("新建立了一个消费者");
	}
	
	/**
	 * 给消费者分配一条消息
	 * @param message
	 */
	public void putMessage(Message message){
		if( ConsumerState.WORKING.getValue().equals(state)) {
			try {
				messageQueue.put(message);
			} catch (InterruptedException e) {
				logger.error("给消费者【{}】分配消息【{}】失败【{}】", this, message, e);
			}
		} else {
			logger.error("给消费者【{}】分配消息【{}】失败，消费者状态错误", this, message);
		}
	}
	
	@Override
	public void close() throws JMSException {
		state = ConsumerState.DEATH.getValue();
		messageQueue.clear();
		messageQueue = null;
	}

	@Override
	public MessageListener getMessageListener() throws JMSException {
		return messageListener;
	}

	@Override
	public String getMessageSelector() throws JMSException {
		return messageSelector;
	}

	@Override
	public Message receive() throws JMSException {
		return receive(0);
	}

	@Override
	public Message receive(long time) throws JMSException {
		return null;
	}

	@Override
	public Message receiveNoWait() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setMessageListener(MessageListener messageListener) throws JMSException {
		if(messageListener == null) {
			throw new JMSException("消息监听器不能为空！");
		}
		
		this.messageListener = messageListener;
	}


	public String getConsumerId() {
		return consumerId;
	}

	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}


	public Destination getDestination() {
		return destination;
	}

	public BlockingQueue<Message> getMessageQueue() {
		return messageQueue;
	}
	
	public String getState() {
		return state;
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
		private SugarMQMessageConsumer consumer;
		
		private Logger logger = LoggerFactory.getLogger(ConsumeMessageTask.class);
		
		public ConsumeMessageTask(SugarMQMessageConsumer consumer) {
			this.consumer = consumer;
		}

		@Override
		public void run() {
			try {
				MessageListener listener = consumer.getMessageListener();
				if(listener == null) {
					logger.error("消费者{}没有配置消息监听器！", consumer);
					return ;
				}
				
				BlockingQueue<Message> queue = consumer.getMessageQueue();
				Message message = null;
				while(!Thread.currentThread().isInterrupted() && 
						ConsumerState.WORKING.getValue().equals(consumer.getState())) {
					try {
						message = queue.poll();
						if(message == null) {
							
						} else {
							listener.onMessage(queue.take());
						}
					} catch (InterruptedException e) {
						logger.error("消费者【{}】消费消息线程被中断【{}】", consumer, e);
						break ;
					}
				}
				
			} catch (JMSException e) {
				logger.error("消费者【{}】消费消息失败：【{}】", consumer, e);
			}
			
		}
	}

}
