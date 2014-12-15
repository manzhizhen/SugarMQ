/**
 * 
 */
package com.sugarmq.transport;

import java.util.ArrayList;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sugarmq.constant.MessageDispatchType;
import com.sugarmq.transport.tcp.TcpMessageTransport;

/**
 * 消息分发的实现类
 * @author manzhizhen
 *
 */
public class MessageDispatcher {
	private SugarMQTransport sugarMQTransport;
	private ArrayList<MessageConsumer> consumerList = new ArrayList<MessageConsumer>(10); // 消费者列表
	private int nextIndex = 0;	// 下一个消费者索引
	
	private Logger logger = LoggerFactory.getLogger(TcpMessageTransport.class);
	
	/**
	 * @param dispatchType
	 * @param acknowledgeType
	 */
	public MessageDispatcher(SugarMQTransport sugarMQTransport) {
		this.sugarMQTransport = sugarMQTransport;
	}
	
	/**
	 * 注册一个消费者
	 * @param messageConsumer
	 */
	public void addConsumer(MessageConsumer messageConsumer) {
		consumerList.add(messageConsumer);
	}
	
	/**
	 * 分发一条消息到消费者
	 * @param message
	 * @throws JMSException 
	 */
	public void dispatchOneMessage(Message message) throws JMSException {
		if(consumerList.isEmpty()) {
			logger.error("有消息到来，却没有消费者:" + message);
			return ;
		}
		
		if(sugarMQTransport.getDispatchType() == null || MessageDispatchType.IN_TURN.getValue().equals(sugarMQTransport.getDispatchType())) {
			if(nextIndex < consumerList.size()) {
				MessageListener messageListener = consumerList.get(nextIndex).getMessageListener();
				if(messageListener != null) {
					messageListener.onMessage(message);
				}
				
			} else {
				consumerList.get(0).getMessageListener().onMessage(message);
				nextIndex = 1;
			}
			
			nextIndex = nextIndex < consumerList.size() ? nextIndex : 0;
			
		} else {
			logger.error("不支持的消息分发类型：" + sugarMQTransport.getDispatchType());
		}
	}
}
