package com.sugarmq.test;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.jms.JMSException;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.sugarmq.manager.SugarMQServerManager;


public class WebTest1 {
	public static void main(String[] args) throws IOException, JMSException {
		ApplicationContext applicationContext = new ClassPathXmlApplicationContext("applicationContext.xml");
		SugarMQServerManager sugarMQServerManager = (SugarMQServerManager) applicationContext.getBean("sugarMQServerManager");
		System.out.println(sugarMQServerManager.getUri());
		sugarMQServerManager.start();

		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		
		while(true) {
			String line = reader.readLine();
			if("exit".equals(line)) {
				break;
			}
		}
		
		((ClassPathXmlApplicationContext)applicationContext).close();
	}
}
