/**
 * 
 */
package com.sugarmq.transport.tcp;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 客户端生产者tcp传输器的接收应答消息线程
 * @author manzhizhen
 *
 */
public class TcpMessageTransportSendAcknowledgeThread implements Runnable{
	private volatile ConcurrentHashMap<String, Semaphore> sendAcknowledgeMessageMap = new ConcurrentHashMap<String, Semaphore>(200); // 接收应答消息的Map
	private Socket socket;
	private byte[] objectByte = new byte[com.sugarmq.message.Message.OBJECT_BYTE_SIZE];
	
	private Logger logger = LoggerFactory.getLogger(TcpMessageTransportSendAcknowledgeThread.class);
	
	public TcpMessageTransportSendAcknowledgeThread(final Socket socket) {
		this.socket = socket;
	}
	
	@Override
	public void run() {
		try {
			Message message = null;
			String messageId = null;
			Semaphore semaphore = null;
			Object rcvMsgObj = null;
			ObjectInputStream objectInputStream = null;
			while(!socket.isInputShutdown()) {
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
				messageId = message.getJMSMessageID();
				semaphore = sendAcknowledgeMessageMap.get(messageId);
				if(semaphore != null) {
					semaphore.release();
					sendAcknowledgeMessageMap.remove(messageId);
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	public ConcurrentHashMap<String, Semaphore> getSendAcknowledgeMessageMap() {
		return sendAcknowledgeMessageMap;
	}
}
