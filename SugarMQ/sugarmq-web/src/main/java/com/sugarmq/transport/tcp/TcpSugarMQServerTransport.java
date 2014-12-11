/**
 * 
 */
package com.sugarmq.transport.tcp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.JMSException;
import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.sugarmq.manager.SugarMQQueueManager;
import com.sugarmq.manager.tcp.TcpReceiveThread;
import com.sugarmq.transport.SugarMQServerTransport;

/**
 * @author manzhizhen
 * 
 */
@Component
public class TcpSugarMQServerTransport implements SugarMQServerTransport {
	@Autowired
	private SugarMQQueueManager sugarQueueManager;
	
	private Socket socket;
	
	// 收消息的队列
	private LinkedBlockingQueue<Message> receiveMessageQueue = new LinkedBlockingQueue<Message>();
	// 发消息的队列
	private LinkedBlockingQueue<Message> sendMessageQueue = new LinkedBlockingQueue<Message>();
	
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
		
		if(!socket.isInputShutdown()) {
			new Thread().start();
		} else {
			logger.debug("Socket未连接，TcpSugarMQServerTransport开启失败！");
		}
		
	
	}

	/**
	 * 发送一条消息
	 * @param message
	 * @param socket
	 * @throws JMSException 
	 */
	public void sendMessage(Message message, Socket socket) throws JMSException {
		if(socket.isClosed()) {
			logger.info("该socket已经关闭，发送消息失败【{}】", message);
			return ;
		}
		
		if(socketList.contains(socket)) {
			logger.error("该socket未注册，发送消息失败【{}】", message);
			return ;
		}
		
		ByteArrayOutputStream byteArrayOutputStream = null;
		ObjectOutputStream objectOutputStream = null;
		try {
			byteArrayOutputStream = new ByteArrayOutputStream();
			objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			
			objectOutputStream.writeObject(message);
			objectOutputStream.flush();
			
			socket.getOutputStream().write(byteArrayOutputStream.toByteArray());
			byteArrayOutputStream.flush();
			
		} catch (IOException e) {
			logger.error("消息【{}】发送失败失败：{}", message, e);
			throw new JMSException(e.getMessage());
			
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
	
	
	@Override
	public void closed() throws JMSException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public BlockingQueue<Message> getReceiveMessageQueue() {
		return receiveMessageQueue;
	}

	@Override
	public BlockingQueue<Message> getSendMessageQueue() {
		return sendMessageQueue;
	}
	
	

}
