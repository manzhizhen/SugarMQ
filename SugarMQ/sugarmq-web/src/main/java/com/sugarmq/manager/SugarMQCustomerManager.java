/**
 * 
 */
package com.sugarmq.manager;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.sugarmq.constant.MessageProperty;
import com.sugarmq.constant.MessageType;
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
	// key-客户端消费者ID value-SugarMQServerTransport的sendMessageQueue
	private ConcurrentHashMap<String, BlockingQueue<Message>> customerMap = 
			new ConcurrentHashMap<String, BlockingQueue<Message>>();
	
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
		if(StringUtils.isBlank(customerId)) {
			logger.debug("客户端没有填写消费者ID【{}】", message);
			customerId = getNewCustomerId();
		}
		
		customerMap.put(customerId, sendMessageQueue);
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
}
