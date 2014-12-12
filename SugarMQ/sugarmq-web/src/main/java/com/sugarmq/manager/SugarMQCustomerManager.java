/**
 * 
 */
package com.sugarmq.manager;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.JMSException;
import javax.jms.Message;

import com.sugarmq.constant.MessageProperty;
import com.sugarmq.constant.MessageType;

/**
 * 类说明：消费者管理器
 *
 * 类描述：
 * @author manzhizhen
 *
 * 2014年12月12日
 */
public class SugarMQCustomerManager {
	// key-客户端消费者ID value-SugarMQServerTransport的sendMessageQueue
	private CopyOnWriteArrayList<Map.Entry<String, BlockingQueue<Message>>> customerList = 
			new CopyOnWriteArrayList<Map.Entry<String, BlockingQueue<Message>>>();
	
	/**
	 * 新注册一个消费者
	 * @param message
	 * @throws JMSException 
	 */
	public void addCustomer(Message message) throws JMSException {
		if(message == null || !MessageType.CUSTOMER_REGISTER_MESSAGE.getValue().
				equals(message.getStringProperty(MessageProperty.MESSAGE_TYPE.getKey()))) {
			throw new IllegalArgumentException();
		}
		
		String custer
		
		
	}
}
