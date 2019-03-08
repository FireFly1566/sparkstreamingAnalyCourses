package com.sparkstreaming.kafka;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProducer extends Thread {

    private String topic;

    private Producer<Integer, String> producer;

    public KafkaProducer(String topic) {
        this.topic = topic;

        Properties properties = new Properties();
        properties.put("metadata.broker.list", KafkaProperties.BROKER_LIST);
        properties.put("serializer.class", "kafka.serializer.StringEncoder");

        /*
         * The number of acknowledgments the producer requires the leader to have received before considering a request complete.
         * This controls the durability of the messages sent by the producer.
         *
         * request.required.acks = 0 - means the producer will not wait for any acknowledgement from the leader.
         * request.required.acks = 1 - means the leader will write the message to its local log and immediately acknowledge
         * request.required.acks = -1 - means the leader will wait for acknowledgement from all in-sync replicas before acknowledging the write
         */
        properties.put("request.required.acks", 1);

        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }

    @Override
    public void run() {
        int messageNo = 1;

        while (true) {
            String message = "message_" + messageNo;
            producer.send(new KeyedMessage<Integer, String>(topic, message));

            System.out.println("发送的消息是： " + message);

            messageNo++;

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
