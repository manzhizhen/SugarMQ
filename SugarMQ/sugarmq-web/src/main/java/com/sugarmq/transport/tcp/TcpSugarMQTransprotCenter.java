/**
 * 
 */
package com.sugarmq.transport.tcp;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.sugarmq.manager.tcp.TcpReceiveThread;

/**
 * 类说明：
 *
 * 类描述：
 * @author manzhizhen
 *
 * 2014年12月11日
 */
public class TcpSugarMQTransprotCenter {
	private InetAddress inetAddress;
	private int port;
	private ServerSocket serverSocket;
	private ConcurrentHashMap<Socket, TcpSugarMQServerTransport> transprotMap = 
			new ConcurrentHashMap<Socket, TcpSugarMQServerTransport>();
	private @Value("${transport-backlog}") int backlog;
	
	private Logger logger = LoggerFactory.getLogger(TcpSugarMQTransprotCenter.class);
	
	public TcpSugarMQTransprotCenter(InetAddress inetAddress, int port) {
		if(inetAddress == null) {
			logger.error("InetAddress为空，创建TcpSugarMQTransprotCenter失败！");
			throw new IllegalArgumentException("InetAddress不能为空！");
		}
		
		this.inetAddress = inetAddress;
		this.port = port;
	}
	
	public void start() throws JMSException {
		try {
			serverSocket = new ServerSocket(port, backlog, inetAddress);
		} catch (IOException e) {
			logger.error("ServerSocket初始化失败：{}", e);
			throw new JMSException(String.format("TcpSugarMQServerTransport绑定URI出错：【%s】【%s】【%s】", 
					new Object[]{inetAddress, port, e.getMessage()}));
		}
		
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
}
