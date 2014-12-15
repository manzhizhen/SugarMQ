/**
 * 
 */
package com.sugarmq.transport;

import java.util.concurrent.BlockingQueue;

import javax.jms.JMSException;
import javax.jms.Message;


/**
 * 客户端传送器的接口
 * @author manzhizhen
 * 
 */
public interface SugarMQTransport {
/*	public String dispatchType; // 消息分发类型
	public int acknowledgeType; // 消息应答类型
	
	public abstract void sendMessage(Message message) throws JMSException;*/
	
	/**
	 * 接收消息
	 * @param time 接收超时时间，如果设置为0，表示永不超时，单位为毫秒
	 * @return
	 * @throws JMSException
	 */
/*	public abstract Message receiveMessage(long time) throws JMSException;

	public abstract void connect() throws JMSException;
	
	public abstract void close() throws JMSException;
	
	public abstract boolean isConnected() throws JMSException;
	
	public abstract boolean isClosed() throws JMSException;
	
	public void setDispatchType(String dispatchType) {
		this.dispatchType = dispatchType;
	}

	public void setAcknowledgeType(int acknowledgeType) {
		this.acknowledgeType = acknowledgeType;
	}

	public String getDispatchType() {
		return dispatchType;
	}
	
	public int getAcknowledgeType() {
		return acknowledgeType;
	}*/
	
	/**
	 * 开启传送通道
	 * @throws JMSException
	 */
	public void start() throws JMSException;
	
	/**
	 * 关闭传送通道
	 * @throws JMSException
	 */
	public void close() throws JMSException;
	
	/**
	 * 获取收到的消息的队列
	 * @return
	 */
	public BlockingQueue<Message> getReceiveMessageQueue();
	
	/**
	 * 获取要发送消息的队列
	 * @return
	 */
	public BlockingQueue<Message> getSendMessageQueue();
}
