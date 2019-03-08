package com.sparkstreaming.kafka;

/**
 * Kafka Java API 测试
 *
 *
 * kafka-console-consumer.sh --bootstrap-server node1:9093,node1:9094,node1:9095 --from-beginning --topic my-replicated-topic
 */

public class KafkaClientApp {

    public static void main(String[] args) {
        new KafkaProducer(KafkaProperties.TOPIC).start();
    }

}
