/**
 * 
 */
package com.sugarmq.transport.tcp;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sugarmq.constant.MessageType;
import com.sugarmq.message.bean.SugarMQMessage;
import com.sugarmq.transport.MessageDispatcher;


/**
 * 客户端tcp传输器的接收消息线程
 * @author manzhizhen
 *
 */
public class TcpMessageTransportReceiveThread implements Runnable{
//	private volatile LinkedBlockingQueue<Message> receiveMessageQueue = new LinkedBlockingQueue<Message>(200); // 待分发的消息队列
	private Socket socket;
	private MessageDispatcher messageDispatch;	// 消息分发器
	private LinkedBlockingQueue<Message> receiveAcknowledgeMessageQueue;
	private byte[] objectByte = new byte[com.sugarmq.message.Message.OBJECT_BYTE_SIZE];
	
	private Logger logger = LoggerFactory.getLogger(TcpMessageTransportReceiveThread.class);
	
	public TcpMessageTransportReceiveThread(final Socket socket, final MessageDispatcher messageDispatch, LinkedBlockingQueue<Message> receiveAcknowledgeMessageQueue) {
		this.socket = socket;
		this.messageDispatch = messageDispatch;
		this.receiveAcknowledgeMessageQueue = receiveAcknowledgeMessageQueue;
	}
	
	@Override
	public void run() {
		try {
			ObjectInputStream objectInputStream = null;
			Message message = null;
			Object rcvMsgObj = null;
			while(!socket.isInputShutdown()) {
				int byteNum = socket.getInputStream().read(objectByte);
				if(byteNum <= 0 ) {
					continue;
				}
				
				objectInputStream = new ObjectInputStream(new ByteArrayInputStream(objectByte, 0, byteNum));
				rcvMsgObj = objectInputStream.readObject();
				
				if(!(rcvMsgObj instanceof Message)) {
					logger.warn("客户端接收到一个非法消息：" + rcvMsgObj);
					continue ;
				}
				
				message = (Message) rcvMsgObj;
//				receiveMessageQueue.put(message);
				messageDispatch.dispatchOneMessage(message);
				
				Message acknowledgeMsg = new SugarMQMessage();
				acknowledgeMsg.setJMSMessageID(message.getJMSMessageID());
				acknowledgeMsg.setJMSType(MessageType.CUSTOMER_ACKNOWLEDGE_MESSAGE.getValue());	// 设置消息类型
				// 接收消息应答
				receiveAcknowledgeMessageQueue.put(acknowledgeMsg);
			}
			
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

//	public LinkedBlockingQueue<Message> getSendMessageQueue() {
//		return receiveMessageQueue;
//	}
	
	

}
