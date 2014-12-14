/**
 * 
 */
package com.sugarmq.dispatch;

import javax.jms.JMSException;
import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sugarmq.manager.SugarMQCustomerManager;
import com.sugarmq.queue.SugarMQMessageContainer;

/**
 * 类说明：消费者消息分发器
 *
 * 类描述：消费者消息分发器
 * @author manzhizhen
 *
 * 2014年12月12日
 */
public class SugarMQCustomerDispatcher {
	private SugarMQCustomerManager sugarMQCustomerManager;
	private SugarMQMessageContainer sugarMQMessageContainer;
	private Thread thread;
	
	private static Logger logger = LoggerFactory.getLogger(SugarMQCustomerDispatcher.class);

	public SugarMQCustomerDispatcher(SugarMQCustomerManager sugarMQCustomerManager, 
			SugarMQMessageContainer sugarMQMessageContainer) {
		if(sugarMQCustomerManager == null) {
			throw new IllegalArgumentException("SugarMQCustomerManager不能为空！");
		}
		
		if(sugarMQMessageContainer == null) {
			throw new IllegalArgumentException("SugarMQMessageContainer不能为空！");
		}
		
		this.sugarMQCustomerManager = sugarMQCustomerManager;
		this.sugarMQMessageContainer = sugarMQMessageContainer;
	}

	public void setSugarMQMessageContainer(SugarMQMessageContainer sugarMQMessageContainer) {
		if(sugarMQMessageContainer == null) {
			throw new IllegalArgumentException("SugarMQMessageContainer不能为空！");
		}
		
		this.sugarMQMessageContainer = sugarMQMessageContainer;
	}
	
	public void start() {
		thread = new Thread(new Runnable() {
			@Override
			public void run() {
				Message message = null;
				while(true) {
					try {
						message = sugarMQMessageContainer.takeMessage();
						sugarMQCustomerManager.putMessageToCustomerQueue(message);
					} catch (JMSException e) {
						logger.info("SugarMQCustomerDispatcher被中断！");
						break ;
					}
				}
				
			}
		});
		
		thread.start();
	}
}
