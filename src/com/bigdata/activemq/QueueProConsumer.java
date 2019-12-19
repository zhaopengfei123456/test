package com.bigdata.activemq;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;

/**
 * 
 * ��Ե�ģʽ һ�������߲���һ����Ϣ ֻ�ܱ���һ�����������ѣ������꣬��Ϣ��û����
 */
public class QueueProConsumer {

	static ConnectionFactory connectionFactory=null;
	static {
		// 1.�������ӹ���
		connectionFactory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");
	}
	// ������
	public void queueProducer(U u) {
		MessageProducer producer = null;
		Session session = null;
		Connection connection = null;
		QueueViewMBean queueBean=null;
		try {
			// 2.��ȡ����
			connection = connectionFactory.createConnection();
			// 3.��������
			connection.start();
			/*
			 * 4.��ȡsession (����1���Ƿ���������, 
			 *               ����2����Ϣȷ��ģʽ[ 
			 *               ǩ�վ��������߽��ܵ���Ϣ����Ҫ������Ϣ�����������յ���Ϣ�ˡ�
			 *               ����Ϣ�������յ���ִ�󣬱�����Ϣ��ʧЧ��
			 *               ����������յ���Ϣ�󣬲���ǩ�գ���ô������Ϣ������Ч���ܿ��ܻᱻ�������������ѵ�
			 *               
			 *               AUTO_ACKNOWLEDGE = 1 �Զ�ȷ��
							 CLIENT_ACKNOWLEDGE = 2 �ͻ����ֶ�ȷ��
							 DUPS_OK_ACKNOWLEDGE = 3 �����ظ�ȷ��(ǩ��ǩ������ν�ˣ�ֻҪ�������ܹ������ظ�����Ϣ����)
							 SESSION_TRANSACTED = 0 �����ύ��ȷ�� ])
			 */
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// 5.�������ж���
			Queue queue = session.createQueue("test");
			// 6.������Ϣ������
			producer = session.createProducer(queue);
			// ��Ϣ�־û�
			//producer.setDeliveryMode(DeliveryMode.PERSISTENT);
			// 7.������Ϣ
			//TextMessage textMessage = session.createTextMessage("��ӭ����MQ�������");
			ObjectMessage objectMessage=session.createObjectMessage();
			//��ʱ����  ÿ��Сʱ ��ʱ�ӳ�1��� ÿ���1�뷢��1�Σ��ܹ�����10�� (��Ҫ��activemq.xml ������ schedulerSupport="true")
			/*objectMessage.setStringProperty(ScheduledMessage.AMQ_SCHEDULED_CRON, "0 * * * *");  
			objectMessage.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, 1000);
			objectMessage.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, 1000);  
			objectMessage.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, 10);*/
			
			HashMap<String,String> map=new HashMap<String, String>();
            map.put("param1", "����2");
            map.put("param2", "����2");
            map.put("param3", "�Ա�2");
            map.put("user", u.toString());
			objectMessage.setObject(map);
            //objectMessage.setObject(u);
			 String url = "service:jmx:rmi:///jndi/rmi://localhost:11099/jmxrmi";
			 JMXServiceURL urls = new JMXServiceURL(url);
			 JMXConnector connector = JMXConnectorFactory.connect(urls,null);
			 connector.connect();
			 MBeanServerConnection conn = connector.getMBeanServerConnection();

			 //����brokerName��bҪСЩ����д�ᱨ��
			 ObjectName name = new ObjectName("myDomain:brokerName=broker,type=Broker");
			 BrokerViewMBean mBean = (BrokerViewMBean)MBeanServerInvocationHandler.newProxyInstance
			 (conn, name, BrokerViewMBean.class, true);
			 for(ObjectName na : mBean.getQueues()){
			 queueBean = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(conn, na, QueueViewMBean.class, true);
			 System.out.println("******************************");
			 System.out.println("���е����ƣ�"+queueBean.getName());
			 System.out.println("������ʣ�����Ϣ����"+queueBean.getQueueSize());
			 System.out.println("����������"+queueBean.getConsumerCount());
			 System.out.println("�����е�������"+queueBean.getDequeueCount());

		}
			 String str=(String)queueBean.getName();
			 if(str.equals("test")){
				 if(queueBean.getQueueSize()==10){
					 producer.close();
					 close(null, session, connection,null);
					 
				 }
			 }
			// 8.������Ϣ
			producer.send(objectMessage);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 9.�ر���Դ
			close(producer, session, connection,null);
		}
	}

	// ����������
	public void queueConsumer() {
		Connection connection=null;
		Session session=null;
		MessageConsumer consumer=null;
		Queue queue=null;
		try {
			//ʹ���е��඼�ܹ�������
			((ActiveMQConnectionFactory) connectionFactory).setTrustAllPackages(true);
			//ʹָ���İ�������ܹ�������
			//((ActiveMQConnectionFactory) connectionFactory).setTrustedPackages(new ArrayList(Arrays.asList("com.bigdata.activemq,com.bigdata.hadoop".split(","))));
			// 2.��ȡ����
			connection = connectionFactory.createConnection();
			// 3.��������
			connection.start();
			// 4.��ȡsession (����1���Ƿ���������,����2����Ϣȷ��ģʽ)
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// 5.�������ж���
			queue = session.createQueue("test-queue");
			// 6.������Ϣ������
			consumer = session.createConsumer(queue);
			// 7.������Ϣ
			consumer.setMessageListener(new MessageListener() {

				public void onMessage(Message message) {
					//TextMessage textMessage = (TextMessage) message;
					ObjectMessage objectMessage=(ObjectMessage)message;
					try {
						//System.out.println("���յ���Ϣ:" + textMessage.getText());
						System.out.println("���յ�������Ϣ:" + objectMessage.getObject());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			// 8.�ȴ���������
			System.in.read();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			// 9.�ر���Դ
			close(null, session, connection,consumer);
		}
	}

	public void close(MessageProducer producer, Session session, Connection connection,MessageConsumer consumer) {
		try {
			if (producer != null) {
				producer.close();
			}
			if(consumer!=null){
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
	
	// ��ʱִ������ �趨ָ������task��ָ���ӳ�delay����й̶��ӳ�peroid��ִ��
	int k=0;
	public  void shendtimer() {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 11); // ����ʱ
		calendar.set(Calendar.MINUTE, 0); // ���Ʒ�
		calendar.set(Calendar.SECOND, 0); // ������
		Date time = calendar.getTime();  // �ó�ִ�������ʱ��,�˴�Ϊ�����11��50��00
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				System.out.println("��ʼ����...");
				//����������
				U u=new U();
				u.setCode(k++);
				u.setText("�ı�"+k);
				queueProducer(u);
			}
		}, time, 1000);
	}
	
	public static void main(String[] args) {
		QueueProConsumer prodconsumer=new QueueProConsumer();
		//����������
		//prodconsumer.queueProducer(new U());
		//��ʱ����
		prodconsumer.shendtimer();
		//����������
		//prodconsumer.queueConsumer();
	}
}
