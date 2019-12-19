package com.bigdata.activemq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * 
 * 发布/订阅模式 发布订阅的模式 默认的请情况下：消息的内容不存在服务器， 当生产者发送了一个消息，如果消费者之前没有订阅，就没了
 */
public class TopicProConsumer {

	static ConnectionFactory connectionFactory = null;

	static {
		// 1.创建连接工厂
		connectionFactory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");
	}

	// 生产者
	public void topicProducer() {
		MessageProducer producer = null;
		Session session = null;
		Connection connection = null;
		try {
			// 2.获取连接
			connection = connectionFactory.createConnection();
			// 3.启动连接
			connection.start();
			/* 4.获取session (参数1：是否启动事务, 参数2：消息确认模式) */
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// 5.创建主题对象
			Topic topic = session.createTopic("test-topic");
			// 6.创建消息生产者
			producer = session.createProducer(topic);
			// 7.创建消息
			TextMessage textMessage = session.createTextMessage("欢迎来到MQ世界!");
			// 8.发送消息
			producer.send(textMessage);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 9.关闭资源
			close(producer, session, connection, null);
		}
	}

	public void topicConsumer01() {
		Connection connection = null;
		Session session = null;
		MessageConsumer consumer = null;
		try {
			// 2.获取连接
			connection = connectionFactory.createConnection();
			// 3.启动连接
			connection.start();
			// 4.获取session (参数1：是否启动事务,参数2：消息确认模式)
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// 5.创建主题对象
			Topic topic = session.createTopic("test-topic");
			// 6.创建消息消费者
			consumer = session.createConsumer(topic);
			// 7.监听消息
			consumer.setMessageListener(new MessageListener() {
				public void onMessage(Message message) {
					TextMessage textMessage = (TextMessage) message;
					try {
						System.out.println("消费1--接收到消息:" + textMessage.getText());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			// 8.等待键盘输入
			System.in.read();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 9.关闭资源
			close(null, session, connection, consumer);
		}
	}

	public void topicConsumer02() {
		Connection connection = null;
		Session session = null;
		MessageConsumer consumer = null;
		try {
			// 2.获取连接
			connection = connectionFactory.createConnection();
			// 3.启动连接
			connection.start();
			// 4.获取session (参数1：是否启动事务,参数2：消息确认模式)
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// 5.创建主题对象
			Topic topic = session.createTopic("test-topic");
			// 6.创建消息消费者
			consumer = session.createConsumer(topic);
			// 7.监听消息
			consumer.setMessageListener(new MessageListener() {
				public void onMessage(Message message) {
					TextMessage textMessage = (TextMessage) message;
					try {
						System.out.println("消费2--接收到消息:" + textMessage.getText());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			// 8.等待键盘输入
			System.in.read();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 9.关闭资源
			close(null, session, connection, consumer);
		}
	}
	// 关闭资源
	public void close(MessageProducer producer, Session session, Connection connection, MessageConsumer consumer) {
		try {
			if (producer != null) {
				producer.close();
			}
			if (consumer != null) {
				consumer.close();
			}
			if (session != null) {
				session.close();
			}
			if (connection != null) {
				connection.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		TopicProConsumer topicproconsumer=new TopicProConsumer();
		//消费者订阅
		//topicproconsumer.topicConsumer01();
		//topicproconsumer.topicConsumer02();
		//生产者生产
		topicproconsumer.topicProducer();
		
	}
}
