package com.sugarmq.manager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.sugarmq.transport.SugarMQServerTransport;
import com.sugarmq.transport.SugarMQServerTransportFactory;
import com.sugarmq.transport.SugarMQTransprotCenter;

/**
 * Web端核心管理类
 * 
 * @author Administrator
 * 
 */
@Component
public class SugarMQServerManager {
	private Logger logger = LoggerFactory
			.getLogger(SugarMQServerManager.class);

	private AtomicBoolean START_STATUS = new AtomicBoolean(false);
//	private ExecutorService executorService;

	private @Value("${server_uri}")
	String uri;
	
	@Autowired
	private SugarMQMessageManager sugarQueueManager;
	@Autowired
	private ConnectionPoolManager connectionPoolManager;
	@Autowired
	private SugarMQServerTransportFactory sugarMQServerTransportFactory;
	
	private SugarMQTransprotCenter sugarMQTransprotCenter;

	/**
	 * 启动Sugar提供者
	 * @throws JMSException 
	 */
	public void start() throws JMSException {
		if (START_STATUS.get()) {
			logger.info("Sugar已经成功启动！");
			return;
		}

		connectionPoolManager.init();
		sugarMQTransprotCenter = sugarMQServerTransportFactory
				.createSugarMQTransport(uri);

		// 创建和JVM进程可用内核数一样的线程数
//		executorService = Executors.newFixedThreadPool(Runtime.getRuntime()
//				.availableProcessors());
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					sugarMQTransprotCenter.start();
				} catch (JMSException e) {
					logger.error(e.getMessage());
				}
			}
		}).start();
		
		START_STATUS.set(true);
		logger.info("Sugar启动完毕！");

	}

	public String getUri() {
		return uri;
	}

//	private static void acceptable(SelectionKey key) throws IOException {
//		SocketChannel socketChannel = serverSocketChannel.accept();
//		Socket socket = socketChannel.socket();
//
//		if (!ConnectionPoolManager.useOneConnection()) {
//			logger.warn("连接数超过最大限值：" + ConnectionPoolManager.MAX_CONNECTION_NUM
//					+ " ，将关闭客户端该链接！");
//			socket.close();
//			return;
//		}
//
//		// 设置非阻塞模式
//		socketChannel.configureBlocking(false);
//		// 注册读事件
//		socketChannel.register(selector, SelectionKey.OP_READ);
//	}
//
//	private static void readable(SelectionKey key) throws IOException,
//			ClassNotFoundException {
//		// 获得与客户端通信的信道
//		SocketChannel socketChannel = (SocketChannel) key.channel();
//
//		if (!socketChannel.isOpen() || !socketChannel.isConnected()) {
//			logger.debug("远程服务器关闭了连接！");
//			socketChannel.close();
//			return;
//		}
//
//		ObjectInputStream objectInputStream = null;
//		try {
//			objByteBuffer.clear();
//			int count = socketChannel.read(objByteBuffer);
//			if (count <= 0) {
//				socketChannel.close();
//				return;
//			}
//
//			objByteBuffer.flip();
//
//			objectInputStream = new ObjectInputStream(new ByteInputStream(
//					objByteBuffer.array(), objByteBuffer.limit()));
//			Message message = (Message) objectInputStream.readObject();
//
//			System.out.println(message);
//
//		} finally {
//			if (objectInputStream != null) {
//				objectInputStream.close();
//			}
//		}
//
//	}
	
}
