package activemq;


import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;



/**
 * 主题消息-接收（消费）者
 */
public class TopicConsumer {
	
	/** 默认用户名 */
	public static final String USERNAME = ActiveMQConnection.DEFAULT_USER;
	/** 默认密码 */
	public static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;
	/** 默认连接地址（格式如：tcp://IP:61616） */
	public static final String BROKER_URL = "tcp://192.168.1.103:61616";	
	
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Destination destination;
    private MessageConsumer messageConsumer;

    public static void main(String[] args) {
        /**
         * Pub/Sub模型中，消息可被多个对象接收，不同于P2P模型
         */
        TopicConsumer consumer1 = new TopicConsumer();
        consumer1.doReceive();
//        TopicConsumer consumer2 = new TopicConsumer();
//        consumer2.doReceive();
    }

    public void doReceive() {
        try {
            connectionFactory = new ActiveMQConnectionFactory(USERNAME,PASSWORD,BROKER_URL);
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            destination = session.createTopic(TopicProducer.TOPIC_NAME);
            messageConsumer = session.createConsumer(destination);

            // 方式一：监听接收
            receiveByListener();

            // 方式二：阻塞接收
            // receiveByManual();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 通过注册监听器的方式接收消息，属于被动监听
     */
    private void receiveByListener() {
        try {
            messageConsumer.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message message) {
                    if (message instanceof TextMessage) {
                        try {
                            TextMessage msg = (TextMessage) message;
                            System.out.println("Received:“" + msg.getText()
                                    + "”");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过手动去接收消息的方式，属于主动获取
     */
    private void receiveByManual() {
        while (true) {
            try {
                /**
                 * 通过receive()方法阻塞接收消息，参数为超时时间（单位：毫秒）
                 */
                TextMessage message = (TextMessage) messageConsumer
                        .receive(60000);
                if (message != null) {
                    System.out.println("Received:“" + message.getText() + "”");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}

