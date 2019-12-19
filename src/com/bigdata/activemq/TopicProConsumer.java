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
 * ����/����ģʽ �������ĵ�ģʽ Ĭ�ϵ�������£���Ϣ�����ݲ����ڷ������� �������߷�����һ����Ϣ�����������֮ǰû�ж��ģ���û��
 */
public class TopicProConsumer {

	static ConnectionFactory connectionFactory = null;

	static {
		// 1.�������ӹ���
		connectionFactory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");
	}

	// ������
	public void topicProducer() {
		MessageProducer producer = null;
		Session session = null;
		Connection connection = null;
		try {
			// 2.��ȡ����
			connection = connectionFactory.createConnection();
			// 3.��������
			connection.start();
			/* 4.��ȡsession (����1���Ƿ���������, ����2����Ϣȷ��ģʽ) */
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// 5.�����������
			Topic topic = session.createTopic("test-topic");
			// 6.������Ϣ������
			producer = session.createProducer(topic);
			// 7.������Ϣ
			TextMessage textMessage = session.createTextMessage("��ӭ����MQ����!");
			// 8.������Ϣ
			producer.send(textMessage);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 9.�ر���Դ
			close(producer, session, connection, null);
		}
	}

	public void topicConsumer01() {
		Connection connection = null;
		Session session = null;
		MessageConsumer consumer = null;
		try {
			// 2.��ȡ����
			connection = connectionFactory.createConnection();
			// 3.��������
			connection.start();
			// 4.��ȡsession (����1���Ƿ���������,����2����Ϣȷ��ģʽ)
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// 5.�����������
			Topic topic = session.createTopic("test-topic");
			// 6.������Ϣ������
			consumer = session.createConsumer(topic);
			// 7.������Ϣ
			consumer.setMessageListener(new MessageListener() {
				public void onMessage(Message message) {
					TextMessage textMessage = (TextMessage) message;
					try {
						System.out.println("����1--���յ���Ϣ:" + textMessage.getText());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			// 8.�ȴ���������
			System.in.read();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 9.�ر���Դ
			close(null, session, connection, consumer);
		}
	}

	public void topicConsumer02() {
		Connection connection = null;
		Session session = null;
		MessageConsumer consumer = null;
		try {
			// 2.��ȡ����
			connection = connectionFactory.createConnection();
			// 3.��������
			connection.start();
			// 4.��ȡsession (����1���Ƿ���������,����2����Ϣȷ��ģʽ)
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// 5.�����������
			Topic topic = session.createTopic("test-topic");
			// 6.������Ϣ������
			consumer = session.createConsumer(topic);
			// 7.������Ϣ
			consumer.setMessageListener(new MessageListener() {
				public void onMessage(Message message) {
					TextMessage textMessage = (TextMessage) message;
					try {
						System.out.println("����2--���յ���Ϣ:" + textMessage.getText());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			// 8.�ȴ���������
			System.in.read();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 9.�ر���Դ
			close(null, session, connection, consumer);
		}
	}
	// �ر���Դ
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
		//�����߶���
		//topicproconsumer.topicConsumer01();
		//topicproconsumer.topicConsumer02();
		//����������
		topicproconsumer.topicProducer();
		
	}
}
