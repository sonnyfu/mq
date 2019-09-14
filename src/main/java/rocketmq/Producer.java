package rocketmq;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class Producer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("sonny");
       // producer.setCreateTopicKey("hello");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        for (int i = 0; i < 5; i ++) {
            Message message = new Message("user", "push", String.valueOf(i), new String("辛星-" + i).getBytes());
            SendResult result = producer.send(message);
            System.out.println("消息id为:  " + result.getMsgId() + "  发送状态为:" + result.getSendStatus());
        }
    }	
	
}
