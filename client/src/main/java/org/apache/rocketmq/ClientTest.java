package org.apache.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;
import java.util.Set;

public class ClientTest {


    public static void main(String[] args) throws Exception {
        get();
    }

    private static void send() throws Exception {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer();
        defaultMQProducer.setNamesrvAddr("127.0.0.1:9876");
        defaultMQProducer.setProducerGroup("xujieProducerGroup");
        defaultMQProducer.start();
        defaultMQProducer.createTopic(defaultMQProducer.getCreateTopicKey(), "xujietopic", 3);
        Message message = new Message("xujietopic", "徐傑的消息".getBytes());
        SendResult sendResult = defaultMQProducer.send(message);
        System.out.println(sendResult.toString());
    }

    private static void get() throws Exception {
        DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer();
        defaultMQPullConsumer.setConsumerGroup("xujieProducerGroup");
        defaultMQPullConsumer.setNamesrvAddr("127.0.0.1:9876");
        defaultMQPullConsumer.start();
        Set<MessageQueue> mqs = defaultMQPullConsumer.fetchSubscribeMessageQueues("xujietopic");

        for (MessageQueue messageQueue : mqs) {
            long offset = defaultMQPullConsumer.fetchConsumeOffset(messageQueue, true);
            System.out.println("consumer from the queue:" + messageQueue + ":" + offset);
            PullResult pullResult = defaultMQPullConsumer.pullBlockIfNotFound(messageQueue, null, 0, 32);
            List<MessageExt> messageExts = pullResult.getMsgFoundList();
            String value = new String(messageExts.get(0).getBody());
            System.out.println("3213");
        }

    }
}
