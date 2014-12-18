package com.sugarmq.consumer;


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
	
	private MessageListener messageListener;
	
	private Logger logger = LoggerFactory.getLogger(SugarMQMessageConsumer.class);
	
	public SugarMQMessageConsumer(Destination destination){
		if(destination == null) {
			throw new IllegalArgumentException("创建消费者失败，Destination为空！");
		}
		
		this.destination = destination;
		
		// 设置状态为创建状态
		state = ConsumerState.CREATE.getValue();
	}
	

	@Override
	public void close() throws JMSException {
		// TODO Auto-generated method stub

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



	
	

}
