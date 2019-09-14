package activemq;


import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * 主题消息-发送（生产）者
 */
public class TopicProducer {

	/** 默认用户名 */
	public static final String USERNAME = ActiveMQConnection.DEFAULT_USER;
	/** 默认密码 */
	public static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;
	/** 默认连接地址（格式如：tcp://IP:61616） */
	public static final String BROKER_URL = "tcp://192.168.1.103:61616";
	/** 队列名称 */
	public static final String TOPIC_NAME = "hello amq-topic";

	// 连接工厂（在AMQ中由ActiveMQConnectionFactory实现）
	private ConnectionFactory connectionFactory;

	// 连接对象
	private Connection connection;

	// 会话对象
	private Session session;

	// 消息目的地（对于点对点模型，是Queue对象；对于发布订阅模型，是Topic对象；它们都继承或实现了该接口）
	private Destination destination;

	// 消息发送（生产）者
	private MessageProducer messageProducer;

	public static void main(String[] args) {
		TopicProducer producer = new TopicProducer();
		producer.doSend();
	}

	public void doSend() {
		try {

			connectionFactory = new ActiveMQConnectionFactory(USERNAME,PASSWORD,BROKER_URL);

			/**
			 * 2.创建连接
			 */
			connection = connectionFactory.createConnection();

			/**
			 * 3.启动连接
			 */
			connection.start();

			session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);

			destination = session.createTopic(TOPIC_NAME);

			messageProducer = session.createProducer(destination);
			
			//messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

			/**
			 * 其他操作：设置消息的存活时间（单位：毫秒）
			 */
			//messageProducer.setTimeToLive(60000);

			/**
			 * 7.创建文本消息<br>
			 * 此外，还有多种类型的消息如对象，字节……都可以通过session.createXXXMessage()方法创建
			 */
			TextMessage message = session
					.createTextMessage("send topic content");

			/**
			 * 8. 发送
			 */
			messageProducer.send(message);

			System.out.println("消息发送完成！");
			/**
			 * 如果有事务操作也可以提交事务
			 */
			session.commit();

			messageProducer.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				try {
					/**
					 * 10.关闭连接（将会关闭程序）
					 */
					connection.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}
}

