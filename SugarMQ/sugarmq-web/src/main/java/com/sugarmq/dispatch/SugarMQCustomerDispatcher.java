/**
 * 
 */
package com.sugarmq.dispatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	private SugarMQMessageContainer sugarMQMessageContainer;
	
	private static Logger logger = LoggerFactory.getLogger(SugarMQCustomerDispatcher.class);
	
	public SugarMQCustomerDispatcher() {
	}
	
	public SugarMQCustomerDispatcher(SugarMQMessageContainer sugarMQMessageContainer) {
		if(sugarMQMessageContainer == null) {
			throw new IllegalArgumentException("SugarMQMessageContainer为空！");
		}
		
		this.sugarMQMessageContainer = sugarMQMessageContainer;
	}

	public void setSugarMQMessageContainer(SugarMQMessageContainer sugarMQMessageContainer) {
		if(sugarMQMessageContainer == null) {
			throw new IllegalArgumentException("SugarMQMessageContainer为空！");
		}
		
		this.sugarMQMessageContainer = sugarMQMessageContainer;
	}
	
	public void start() {
		
	}
}
