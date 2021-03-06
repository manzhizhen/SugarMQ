package com.sugarmq.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.sugarmq.constant.MessageContainerType;
import com.sugarmq.core.SugarMQConnectionFactory;
import com.sugarmq.message.SugarMQDestination;

public class ClientTest1 {
	public static void main(String[] args) {
		try {
			SugarMQConnectionFactory facotory = new SugarMQConnectionFactory("tcp://10.79.6.181:1314");
			Connection connection = facotory.createConnection();
			connection.start();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			Queue queue = new SugarMQDestination("manzhizhen", MessageContainerType.QUEUE.getValue());
			
			MessageConsumer consumer = session.createConsumer(queue);
			consumer.setMessageListener(new MessageListener() {
				@Override
				public void onMessage(Message msg) {
					try {
						System.out.println("consumer1：" + ((TextMessage)msg).getText());
					} catch (JMSException e) {
						e.printStackTrace();
					}
				}
			});
			
			MessageConsumer consumer1 = session.createConsumer(queue);
			consumer1.setMessageListener(new MessageListener() {
				@Override
				public void onMessage(Message msg) {
					try {
						System.out.println("consumer2：" + ((TextMessage)msg).getText());
					} catch (JMSException e) {
						e.printStackTrace();
					}
				}
			});
			
			MessageConsumer consumer2 = session.createConsumer(queue);
			consumer2.setMessageListener(new MessageListener() {
				@Override
				public void onMessage(Message msg) {
					try {
						System.out.println("consumer3：" + ((TextMessage)msg).getText());
					} catch (JMSException e) {
						e.printStackTrace();
					}
				}
			});
			
			System.out.println("消费者创建完毕！");
			
			TextMessage textMessage = session.createTextMessage();
			textMessage.setText("Do you love me?");
			
			MessageProducer sender = session.createProducer(queue);
			for(int i = 0; i < 6; i++) {
				sender.send(textMessage);
			}
			System.out.println("消息发送完毕！！！");
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			while(true) {
				reader.readLine();
			}
		} catch (JMSException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
}
