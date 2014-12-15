/**
 * 
 */
package com.sugarmq.transport.tcp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.JMSException;
import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sugarmq.transport.SugarMQServerTransport;

/**
 * 
 * 
 * @author manzhizhen
 * 
 */
public class TcpSugarMQServerTransport implements SugarMQServerTransport {
	
	private Socket socket;
	
	// 收消息的队列
	private LinkedBlockingQueue<Message> receiveMessageQueue = new LinkedBlockingQueue<Message>();
	// 发消息的队列
	private LinkedBlockingQueue<Message> sendMessageQueue = new LinkedBlockingQueue<Message>();
	
	private Thread sendMessageThread;
	private Thread receiveMessageThread;
	
	private byte[] objectByte = new byte[com.sugarmq.message.Message.OBJECT_BYTE_SIZE];
	
	private Logger logger = LoggerFactory.getLogger(TcpSugarMQServerTransport.class);
	
	public TcpSugarMQServerTransport(Socket socket) {
		if(socket == null) {
			logger.error("Socket对象不能为空！");
			throw new IllegalArgumentException("Socket不能为空！");
		}
		
		this.socket = socket;
	}
	
	@Override
	public void start() throws JMSException {
		if(socket.isClosed()) {
			logger.error("Socket已经关闭，TcpSugarMQServerTransport开启失败！");
			throw new JMSException("Socket已经关闭，TcpSugarMQServerTransport开启失败！");
		}
		
		if(!socket.isConnected()) {
			logger.error("Socket未连接，TcpSugarMQServerTransport开启失败！");
			throw new JMSException("Socket未连接，TcpSugarMQServerTransport开启失败！");
		}
		
		// 消息接收线程
		if(!socket.isInputShutdown()) {
			receiveMessageThread = new Thread(
				new Runnable() {
					@Override
					public void run() {
						receiveMessage();
					}
				}
			);
			
			receiveMessageThread.start();
		} else {
			logger.debug("Socket未连接，TcpSugarMQServerTransport开启消息接收线程失败！");
		}
		
		// 消息发送线程
		if(!socket.isOutputShutdown()) {
			sendMessageThread = new Thread(
				new Runnable() {
					@Override
					public void run() {
						sendMessage();
					}
				}
			);
			
			sendMessageThread.start();
		} else {
			logger.debug("Socket未连接，TcpSugarMQServerTransport开启消息发送线程失败！");
		}
	}
	
	@Override
	public void close() {
		logger.debug("TcpSugarMQServerTransport即将被关闭！");
		
		if(sendMessageThread != null && Thread.State.TERMINATED != sendMessageThread.getState()) {
			sendMessageThread.interrupt();
		}
		
		if(receiveMessageThread != null && Thread.State.TERMINATED != receiveMessageThread.getState()) {
			receiveMessageThread.interrupt();
		}
		
		try {
			socket.close();
		} catch (IOException e) {
			logger.info("Socket关闭异常", e);
		}
		
		sendMessageQueue.clear();
		receiveMessageQueue.clear();
		
		logger.debug("TcpSugarMQServerTransport已被关闭！");
	}

	@Override
	public BlockingQueue<Message> getReceiveMessageQueue() {
		return receiveMessageQueue;
	}

	@Override
	public BlockingQueue<Message> getSendMessageQueue() {
		return sendMessageQueue;
	}
	
	/**
	 * 从Socket中接收消息
	 */
	private void receiveMessage() {
		try {
			ObjectInputStream objectInputStream = null;
			Message message = null;
			Object rcvMsgObj = null;
			while(!Thread.currentThread().isInterrupted() && !socket.isClosed() && !socket.isInputShutdown()) {
				int byteNum = socket.getInputStream().read(objectByte);
				if(byteNum <= 0 ) {
					continue;
				}
				
				objectInputStream = new ObjectInputStream(new ByteArrayInputStream(objectByte, 0, byteNum));
				rcvMsgObj = objectInputStream.readObject();
				
				if(!(rcvMsgObj instanceof Message)) {
					logger.warn("服务端接收到一个非法消息：" + rcvMsgObj);
					continue ;
				}
				
				message = (Message) rcvMsgObj;
				logger.info("服务端接收到客户端发来的一条消息:{}", message);
				
				receiveMessageQueue.put(message);
			}
			
			logger.error("Socket状态异常，TcpSugarMQServerTransport接收消息线程结束！");
		} catch (Exception e) {
			logger.error("TcpSocketThread线程错误：{}", e);
		}
	}
	
	/**
	 * 发送消息
	 */
	private void sendMessage() {
		Message message = null;
		ByteArrayOutputStream byteArrayOutputStream = null;
		ObjectOutputStream objectOutputStream = null;
		while(true) {
			try {
				message = sendMessageQueue.take();
			} catch (InterruptedException e1) {
				logger.info("TcpSocketThread发送线程被要求停止！");
				break ;
			}
			
			if(!Thread.currentThread().isInterrupted() && !socket.isClosed() && !socket.isOutputShutdown()) {
				try {
					byteArrayOutputStream = new ByteArrayOutputStream();
					objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
					
					objectOutputStream.writeObject(message);
					objectOutputStream.flush();
					
					socket.getOutputStream().write(byteArrayOutputStream.toByteArray());
					byteArrayOutputStream.flush();
					
				} catch (IOException e) {
					logger.error("消息【{}】发送失败失败：{}", message, e);
					
				} finally {
					if(objectOutputStream != null) {
						try {
							objectOutputStream.close();
						} catch (IOException e) {
						}
					}
					
					if(byteArrayOutputStream != null) {
						try {
							byteArrayOutputStream.close();
						} catch (IOException e) {
						}
					}
				}
			}
			
		}
		
		logger.info("TcpSugarMQServerTransport消息发送线程结束！");
	}
	
	

}
