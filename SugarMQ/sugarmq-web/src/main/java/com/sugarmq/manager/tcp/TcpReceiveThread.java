/**
 * 
 */
package com.sugarmq.manager.tcp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.sugarmq.constant.MessageType;
import com.sugarmq.manager.SugarMQQueueManager;
import com.sugarmq.transport.tcp.TcpMessageTransport;
import com.sugarmq.transport.tcp.TcpSugarMQServerTransport;

/**
 * 服务端消息接收线程
 * @author manzhizhen
 *
 */
public class TcpReceiveThread implements Runnable{
	private Socket socket;
	private SugarMQQueueManager sugarQueueManager;
	private TcpSugarMQServerTransport tcpSugarMQServerTransport;
	private byte[] objectByte = new byte[com.sugarmq.message.Message.OBJECT_BYTE_SIZE];
	
	private static Logger logger = LoggerFactory.getLogger(TcpReceiveThread.class);
	
	public TcpReceiveThread(SugarMQQueueManager sugarQueueManager, TcpSugarMQServerTransport tcpSugarMQServerTransport) {
		this.sugarQueueManager = sugarQueueManager;
		this.tcpSugarMQServerTransport = tcpSugarMQServerTransport;
	}
	
	@Override
	public void run() {
		try {
			ObjectInputStream objectInputStream = null;
			
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			
			Message message = null;
			String messageType = null;
			Message acknowledgeMessage = null;
			Object rcvMsgObj = null;
			while(!socket.isOutputShutdown() && !socket.isInputShutdown()) {
				int byteNum = socket.getInputStream().read(objectByte);
				if(byteNum <= 0 ) {
					continue;
				}
				
				objectInputStream = new ObjectInputStream(new ByteArrayInputStream(objectByte, 0, byteNum));
				rcvMsgObj = objectInputStream.readObject();
				
				if(!(rcvMsgObj instanceof Message)) {
					logger.warn("客户端接收到一个非法消息应答：" + rcvMsgObj);
					continue ;
				}
				
				message = (Message) rcvMsgObj;
				logger.info("接收到客户端发来的一条消息:{}", message);
				
				messageType = message.getJMSType();
				
				// 如果是生产者发过来的消息
				if(MessageType.PRODUCER_MESSAGE.getValue().equals(messageType)) {
					logger.info("是生产者发过来的消息:{}", message);
					acknowledgeMessage = sugarQueueManager.receiveProducerMessage(message);
					
					
					
					// 应答生产者的消息
//					objectOutputStream.writeObject(acknowledgeMessage);
//					objectOutputStream.flush();
//					
//					socket.getOutputStream().write(byteArrayOutputStream.toByteArray());
//					byteArrayOutputStream.flush();
					
					
				// 如果是消费者发过来的应答消息
				} else if(MessageType.CUSTOMER_ACKNOWLEDGE_MESSAGE.getValue().equals(messageType)) {
					logger.info("是消费者发过来的应答消息:{}", message);
					sugarQueueManager.receiveConsumerAcknowledgeMessage(message);
				}
				
			}
		} catch (Exception e) {
			logger.error("TcpSocketThread线程错误：{}", e);
		}
	}
}
