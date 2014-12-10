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
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.JMSException;
import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.sugarmq.manager.SugarMQQueueManager;
import com.sugarmq.manager.tcp.TcpReceiveThread;
import com.sugarmq.transport.SugarMQServerTransport;

/**
 * @author manzhizhen
 * 
 */
@Scope("prototype")
@Component
public class TcpSugarMQServerTransport implements SugarMQServerTransport {
	private InetAddress inetAddress;
	private int port;
	private @Value("${transport-backlog}") int backlog;
	private ServerSocket serverSocket;
	@Autowired
	private SugarMQQueueManager sugarQueueManager;
	
	private CopyOnWriteArrayList<Socket> socketList = new CopyOnWriteArrayList<Socket>();
	
	private Logger logger = LoggerFactory.getLogger(TcpSugarMQServerTransport.class);

	public void setInetAddress(InetAddress inetAddress) {
		this.inetAddress = inetAddress;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public void bind() throws JMSException {
		if(inetAddress == null) {
			throw new JMSException("inetAddress地址为空，无法绑定端口！");
		}
		
		try {
			serverSocket = new ServerSocket(port, backlog, inetAddress);
		} catch (IOException e) {
			logger.error("ServerSocket初始化失败：{}", e);
			throw new JMSException(String.format("TcpSugarMQServerTransport绑定URI出错：【%s】【%s】【%s】", 
					new Object[]{inetAddress, port, e.getMessage()}));
		}
	}

	@Override
	public void start() throws JMSException {
		while(true) {
			try {
				Socket socket = serverSocket.accept();
				socketList.add(socket);
				new Thread(new TcpReceiveThread(sugarQueueManager, this)).start();
				
			} catch (IOException e) {
				logger.error("TcpSugarMQServerTransport启动失败：", e);
				throw new JMSException(e.getMessage());
			}
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
			socketList.remove(socket);
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
	
	

}
