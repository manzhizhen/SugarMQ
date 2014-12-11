/**
 * 
 */
package com.sugarmq.constant;

/**
 * 类说明：消息类型
 *
 * 类描述：
 * @author manzhizhen
 *
 * 2014年12月10日
 */
public enum MessageType {
	PRODUCER_MESSAGE("PRODUCER_MESSAGE"),	// 生产者发送的消息
	CUSTOMER_ACKNOWLEDGE_MESSAGE("CUSTOMER_ACKNOWLEDGE_MESSAGE"),	// 消费者应答消息
	CUSTOMER_REGISTER_MESSAGE("CUSTOMER_REGISTER_MESSAGE");	// 消费者注册消息
	
	String value;
	private MessageType(String value) {
		this.value = value;
	}
	
	public String getValue() {
		return value;
	}
	
	@Override
	public String toString() {
		return value;
	}
}
