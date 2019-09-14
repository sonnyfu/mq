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
 * 队列消息-接收（消费）者
 */
public class QueueConsumer {

    /** 默认用户名 */
    public static final String USERNAME = ActiveMQConnection.DEFAULT_USER;
    /** 默认密码 */
    public static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;
    /** 默认连接地址（格式如：tcp://IP:61616） */
    /**
     * 单机：192.168.1.103
     * 集群：failover:(tcp://192.168.1.103:61616?wireFormat.maxInactivityDuration=0,tcp://192.168.0.103:61617?wireFormat.maxInactivityDuration=0)
     */
    public static final String BROKER_URL = "failover:(tcp://192.168.1.103:61616?wireFormat.maxInactivityDuration=0)";
    public static final String QUEUE_NAME = "hello amq";
	
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Destination destination;
    private MessageConsumer messageConsumer;

    public static void main(String[] args) {
        QueueConsumer consumer = new QueueConsumer();
        consumer.doReceive();
    }

    public void doReceive() {
        try {
        	//192.168.0.103:61616
        	
        	connectionFactory = new ActiveMQConnectionFactory(USERNAME,PASSWORD,BROKER_URL);
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            destination = session.createQueue(QueueProducer.QUEUE_NAME);
            /**
             * 注意：这里要创建一个消息消费，并指定目的地（即消息源队列）
             */
            messageConsumer = session.createConsumer(destination);

            // 方式一：监听接收
            receiveByListener();

            // 方式二：阻塞接收
           // receiveByManual();

            /**
             * 注意：这里不能再关闭对象了
             */
            // messageConsumer.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // connection.close();
        }

    }

    /**
     * 通过注册监听器的方式接收消息，属于被动监听（回调或推模式）
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
                            // 可以通过此方法反馈消息已收到
                            //msg.acknowledge();//和recovery一样，非事务model，不调用不调用就会导致重发（重复消费），超过6次就会进入DLQ
                            //session.commit();//和rollback一样
                            //若上述问题定位出来，可通过console retry再次消费
                            System.out.println("确认完毕");
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
     * 通过手动去接收消息的方式，属于主动获取（push model）
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
                message.acknowledge();
                System.out.println("确认完毕");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
