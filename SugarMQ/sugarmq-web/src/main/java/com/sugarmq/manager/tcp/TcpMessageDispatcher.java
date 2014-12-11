/**
 * 
 */
package com.sugarmq.manager.tcp;

import java.net.Socket;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.JMSException;
import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.sugarmq.queue.SugarMQQueue;
import com.sugarmq.transport.tcp.TcpSugarMQServerTransport;

/**
 * 分发消息线程
 * 每个队列有一个该线程负责分发消息
 * @author manzhizhen
 *
 */
public class TcpMessageDispatcher {

//	private List<Socket> socketList = new CopyOnWriteArrayList<Socket>();
	// key-客户端消费者ID value-Socket
	private CopyOnWriteArrayList<Map.Entry<String, Socket>> customerList = 
			new CopyOnWriteArrayList<Map.Entry<String, Socket>>();
	private SugarMQQueue queue;
	private int index = 0;
	@Autowired
	private TcpSugarMQServerTransport tcpSugarMQServerTransport;
	
	private static Logger logger = LoggerFactory.getLogger(TcpMessageDispatcher.class);
	
	public TcpMessageDispatcher(SugarMQQueue queue) {
		this.queue = queue;
	}
	
	@Override
	public void run() {
		Message message = null;
		while(true) {
			if(customerList.size() == 0) {
				continue;
			}
			
			if(index >= customerList.size()) {
				index = 0;
			}
			
			while(true) {
				Map.Entry<String, Socket> entry = customerList.get(index);
				
				try {
					message = queue.takeMessage();
					message.setStringProperty("_#_CUSTOMER_ID", entry.getKey());
					
					tcpSugarMQServerTransport.sendMessage(message, entry.getValue());
					
				} catch (JMSException e) {
					logger.error("分发消息【{}】失败：", message, e);
				} finally {
					index++;
				}
			}
		}
	}
}
