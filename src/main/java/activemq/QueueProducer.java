package activemq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * 队列消息-发送（生产）者
 */
public class QueueProducer {

    
    public static final String USERNAME = ActiveMQConnection.DEFAULT_USER;
   
    public static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;
    /** 默认连接地址（格式如：tcp://IP:61616） */
    //failover机制:(tcp://192.168.0.103:61616,tcp://192.168.0.103:61617,tcp://192.168.0.103:61618)
    //static network 192.168.1.104uni-channel到103机器
    public static final String BROKER_URL = "tcp://192.168.1.104:61616";
    /** 队列名称 */
    public static final String QUEUE_NAME = "hello amq";

    // 连接工厂（在AMQ中由ActiveMQConnectionFactory实现）
    private ConnectionFactory connectionFactory;

    private Connection connection;

    private Session session;

    // 消息目的地（对于点对点模型，是Queue对象；对于发布订阅模型，是Topic对象；它们都继承或实现了该接口）
    private Destination destination;

    private MessageProducer messageProducer;

    public static void main(String[] args) {
        QueueProducer producer = new QueueProducer();
        producer.doSend();
    }

    public void doSend() {
        try {

        	connectionFactory = new ActiveMQConnectionFactory(USERNAME,PASSWORD,BROKER_URL);
            connection = connectionFactory.createConnection();

            connection.start();

            /**
             * 4.创建会话<br>
             * param1:是否支持事务，若为true，则会忽略第二个参数，默认为SESSION_TRANSACTED<br>
             * param2:确认消息模式，若第一个参数为false时，该参数有以下几种状态<br>
             * -Session.AUTO_ACKNOWLEDGE：自动确认。
             * 消费者消息不需要做额外的工作，即使接收端发生异常或不调用message.acknowledge() ，也会被当作正常发送成功 <br>
             * -Session.CLIENT_ACKNOWLEDGE：客户端确认。客户端接收到消息后，必须调用message.
             * acknowledge() 方法给予收到反馈，JMS服务器才会把该消息当做发送成功，并删除<br>
             * -Session.DUPS_OK_ACKNOWLEDGE：副本确认。一旦接收端应用程序的方法调用从处理消息处返回，
             * 会话对象就会确认消息的接收(即接收端发生异常ok或不调用message.
             * acknowledge()ok)，而且允许重复确认。
             */
            session = connection.createSession(true, Session.DUPS_OK_ACKNOWLEDGE);

            destination = session.createQueue(QUEUE_NAME);
            messageProducer = session.createProducer(destination);
            /**
             * 其他操作： 设置生产者的生产模式，默认为持久化<
             * 参数有以下两种状态：
			messages are persisted to disk/database so that they will survive a broker restart. 
			When using non-persistent delivery, if you kill a broker then you will lose all in-transit messages.             
             */
            //messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
           messageProducer.setTimeToLive(10*1000);

            for (int i = 0; i < 1; i++) {
                TextMessage message = session.createTextMessage("send content:"
                        + i);
                messageProducer.send(message);
            }
            System.out.println("消息发送完成！");
            /**
             * commit之前只是写到broker的日历里面，commit之后才persist
             */
            session.commit();
            //session.rollback();
            messageProducer.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
