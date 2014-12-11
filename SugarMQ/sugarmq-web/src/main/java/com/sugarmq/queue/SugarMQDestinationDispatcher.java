/**
 * 
 */
package com.sugarmq.queue;

import java.net.Socket;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.sugarmq.constant.MessageProperty;
import com.sugarmq.constant.MessageType;
import com.sugarmq.transport.tcp.TcpSugarMQServerTransport;

/**
 * 分发消息线程
 * 每个队列有一个该线程负责分发消息
 * @author manzhizhen
 *
 */
public class SugarMQDestinationDispatcher {

	// key-客户端消费者ID value-Socket
	private CopyOnWriteArrayList<Map.Entry<String, Socket>> customerList = 
			new CopyOnWriteArrayList<Map.Entry<String, Socket>>();
	private BlockingQueue<Message> receiveMessageQueue;
	private int index = 0;
	
	private static Logger logger = LoggerFactory.getLogger(SugarMQDestinationDispatcher.class);
	
	public SugarMQDestinationDispatcher(BlockingQueue<Message> receiveMessageQueue) {
		if(receiveMessageQueue == null) {
			throw new IllegalArgumentException("BlockingQueue<Message>不能为空！");
		}
		
		this.receiveMessageQueue = receiveMessageQueue;
	}
	
	public void start() {
		logger.info("SugarMQQueueDispatcher已经开始工作.");
		Message message = null;
		while(true) {
			try {
				message = receiveMessageQueue.take();
			} catch (InterruptedException e) {
				logger.info("SugarMQQueueDispatcher被中断，即将退出.");
				break ;
			}
			
			logger.debug("开始处理消息【{}】", message);
			
			try {
				if(MessageType.PRODUCER_MESSAGE.getValue().
						equals(message.getStringProperty(MessageProperty.MESSAGE_TYPE.getKey()))) {
					
					
				}
			} catch (JMSException e) {
				logger.error("SugarMQQueueDispatcher消息处理失败:【{}】", message, e);
			}
		}
	}
}
