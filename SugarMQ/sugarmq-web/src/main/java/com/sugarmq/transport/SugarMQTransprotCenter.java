/**
 * 
 */
package com.sugarmq.transport;

import javax.jms.JMSException;

/**
 * 类说明：
 *
 * 类描述：
 * @author manzhizhen
 *
 * 2014年12月15日
 */
public interface SugarMQTransprotCenter {
	public void start() throws JMSException;
	
	public void close() throws JMSException;
}
