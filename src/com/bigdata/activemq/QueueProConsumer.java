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
 * 点对点模式 一个生成者产生一个消息 只能被被一个消费者消费，消费完，消息就没有了
 */
public class QueueProConsumer {

	static ConnectionFactory connectionFactory=null;
	static {
		// 1.创建连接工厂
		connectionFactory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");
	}
	// 生产者
	public void queueProducer(U u) {
		MessageProducer producer = null;
		Session session = null;
		Connection connection = null;
		QueueViewMBean queueBean=null;
		try {
			// 2.获取连接
			connection = connectionFactory.createConnection();
			// 3.启动连接
			connection.start();
			/*
			 * 4.获取session (参数1：是否启动事务, 
			 *               参数2：消息确认模式[ 
			 *               签收就是消费者接受到消息后，需要告诉消息服务器，我收到消息了。
			 *               当消息服务器收到回执后，本条消息将失效。
			 *               如果消费者收到消息后，并不签收，那么本条消息继续有效，很可能会被其他消费者消费掉
			 *               
			 *               AUTO_ACKNOWLEDGE = 1 自动确认
							 CLIENT_ACKNOWLEDGE = 2 客户端手动确认
							 DUPS_OK_ACKNOWLEDGE = 3 允许重复确认(签不签收无所谓了，只要消费者能够容忍重复的消息接受)
							 SESSION_TRANSACTED = 0 事务提交并确认 ])
			 */
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// 5.创建队列对象
			Queue queue = session.createQueue("test");
			// 6.创建消息生产者
			producer = session.createProducer(queue);
			// 消息持久化
			//producer.setDeliveryMode(DeliveryMode.PERSISTENT);
			// 7.创建消息
			//TextMessage textMessage = session.createTextMessage("欢迎来到MQ世界哈哈");
			ObjectMessage objectMessage=session.createObjectMessage();
			//定时发送  每个小时 定时延迟1秒后 每间隔1秒发送1次，总共发送10次 (需要在activemq.xml 中配置 schedulerSupport="true")
			/*objectMessage.setStringProperty(ScheduledMessage.AMQ_SCHEDULED_CRON, "0 * * * *");  
			objectMessage.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, 1000);
			objectMessage.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, 1000);  
			objectMessage.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, 10);*/
			
			HashMap<String,String> map=new HashMap<String, String>();
            map.put("param1", "姓名2");
            map.put("param2", "年龄2");
            map.put("param3", "性别2");
            map.put("user", u.toString());
			objectMessage.setObject(map);
            //objectMessage.setObject(u);
			 String url = "service:jmx:rmi:///jndi/rmi://localhost:11099/jmxrmi";
			 JMXServiceURL urls = new JMXServiceURL(url);
			 JMXConnector connector = JMXConnectorFactory.connect(urls,null);
			 connector.connect();
			 MBeanServerConnection conn = connector.getMBeanServerConnection();

			 //这里brokerName的b要小些，大写会报错
			 ObjectName name = new ObjectName("myDomain:brokerName=broker,type=Broker");
			 BrokerViewMBean mBean = (BrokerViewMBean)MBeanServerInvocationHandler.newProxyInstance
			 (conn, name, BrokerViewMBean.class, true);
			 for(ObjectName na : mBean.getQueues()){
			 queueBean = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(conn, na, QueueViewMBean.class, true);
			 System.out.println("******************************");
			 System.out.println("队列的名称："+queueBean.getName());
			 System.out.println("队列中剩余的消息数："+queueBean.getQueueSize());
			 System.out.println("消费者数："+queueBean.getConsumerCount());
			 System.out.println("出队列的数量："+queueBean.getDequeueCount());

		}
			 String str=(String)queueBean.getName();
			 if(str.equals("test")){
				 if(queueBean.getQueueSize()==10){
					 producer.close();
					 close(null, session, connection,null);
					 
				 }
			 }
			// 8.发送消息
			producer.send(objectMessage);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 9.关闭资源
			close(producer, session, connection,null);
		}
	}

	// 创建消费者
	public void queueConsumer() {
		Connection connection=null;
		Session session=null;
		MessageConsumer consumer=null;
		Queue queue=null;
		try {
			//使所有的类都能够被传输
			((ActiveMQConnectionFactory) connectionFactory).setTrustAllPackages(true);
			//使指定的包里的类能够被传输
			//((ActiveMQConnectionFactory) connectionFactory).setTrustedPackages(new ArrayList(Arrays.asList("com.bigdata.activemq,com.bigdata.hadoop".split(","))));
			// 2.获取连接
			connection = connectionFactory.createConnection();
			// 3.启动连接
			connection.start();
			// 4.获取session (参数1：是否启动事务,参数2：消息确认模式)
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// 5.创建队列对象
			queue = session.createQueue("test-queue");
			// 6.创建消息消费者
			consumer = session.createConsumer(queue);
			// 7.监听消息
			consumer.setMessageListener(new MessageListener() {

				public void onMessage(Message message) {
					//TextMessage textMessage = (TextMessage) message;
					ObjectMessage objectMessage=(ObjectMessage)message;
					try {
						//System.out.println("接收到消息:" + textMessage.getText());
						System.out.println("接收到对象消息:" + objectMessage.getObject());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			// 8.等待键盘输入
			System.in.read();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			// 9.关闭资源
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
	
	// 定时执行生产 设定指定任务task在指定延迟delay后进行固定延迟peroid的执行
	int k=0;
	public  void shendtimer() {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 11); // 控制时
		calendar.set(Calendar.MINUTE, 0); // 控制分
		calendar.set(Calendar.SECOND, 0); // 控制秒
		Date time = calendar.getTime();  // 得出执行任务的时间,此处为今天的11：50：00
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				System.out.println("开始生产...");
				//调用生产者
				U u=new U();
				u.setCode(k++);
				u.setText("文本"+k);
				queueProducer(u);
			}
		}, time, 1000);
	}
	
	public static void main(String[] args) {
		QueueProConsumer prodconsumer=new QueueProConsumer();
		//调用生产者
		//prodconsumer.queueProducer(new U());
		//定时生产
		prodconsumer.shendtimer();
		//调用消费者
		//prodconsumer.queueConsumer();
	}
}
