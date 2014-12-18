/**
 * 
 */
package com.sugarmq.constant;

/**
 * 提供者用到的消息属性
 * @author manzhizhen
 * 
 */
public enum MessageProperty {
	DISABLE_MESSAGE_ID("_#_disableMessageId", false),
	SESSION_ID("_#_sessionId", null),
	CUSTOMER_ID("_#_customerId", null),
	CUSTOMER_CLIENT_ID("_#_customerClientId", null);
	
	String key;
	Object value;
	private MessageProperty(String key, Object value) {
		this.key = key;
		this.value = value;
	}
	
	public String getKey() {
		return key;
	}
	
	public Object getValue() {
		return value;
	}
	
	@Override
	public String toString() {
		return key;
	}
}
