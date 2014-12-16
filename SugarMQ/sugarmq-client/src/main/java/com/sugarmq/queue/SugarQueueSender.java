package com.sugarmq.queue;


import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueSender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sugarmq.constant.MessageProperty;
import com.sugarmq.constant.MessageType;
import com.sugarmq.core.SugarMQMessageProducer;
import com.sugarmq.message.SugarDestination;
import com.sugarmq.message.bean.SugarMQMessage;
import com.sugarmq.transport.SugarMQTransport;

public class SugarQueueSender extends SugarMQMessageProducer implements QueueSender {
	private SugarMQTransport sugarMQTransport;
	private Destination destination;
	
	private Logger logger = LoggerFactory.getLogger(SugarQueueSender.class);
	
	public SugarQueueSender(Destination destination, SugarMQTransport sugarMQTransport) {
		this.destination = destination;
		this.sugarMQTransport = sugarMQTransport;
	}

	@Override
	public void close() throws JMSException {
		// TODO Auto-generated method stub
	}

	@Override
	public Destination getDestination() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getDisableMessageID() throws JMSException {
		return super.disableMessageId.get();
	}

	@Override
	public boolean getDisableMessageTimestamp() throws JMSException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void send(Destination destination, Message message) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void send(Destination arg0, Message arg1, int arg2, int arg3,
			long arg4) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setDeliveryMode(int arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setDisableMessageID(boolean disableMessageId) throws JMSException {
		super.disableMessageId.set(disableMessageId);
	}

	@Override
	public void setDisableMessageTimestamp(boolean arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public Queue getQueue() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void send(Message message) throws JMSException {
//		if(!(message instanceof SugarMessage)) {
//			logger.warn("传入的Message无法识别！");
//			throw new JMSException("传入的Message无法识别！");
//		}
		
		message.setJMSType(MessageType.PRODUCER_MESSAGE.getValue()); // 设置消息类型
		message.setJMSDestination(destination);
		message.setBooleanProperty(MessageProperty.DISABLE_MESSAGE_ID.getKey(), super.disableMessageId.get());
		
		System.out.println(message.getJMSType());
		
		sugarMQTransport.sendMessage(message);
	}

	@Override
	public void send(Queue arg0, Message arg1) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void send(Message arg0, int arg1, int arg2, long arg3)
			throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void send(Queue arg0, Message arg1, int arg2, int arg3, long arg4)
			throws JMSException {
		// TODO Auto-generated method stub

	}

}
