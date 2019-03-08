package com.sparkstreaming.kafka;

/**
 * Kafka Java API 测试
 *
 * kafka-server-start.sh $KAFKA_HOME/config/server-1.properties &
 * kafka-server-start.sh $KAFKA_HOME/config/server-2.properties &
 * kafka-server-start.sh $KAFKA_HOME/config/server-3.properties &
 *
 *
 * kafka-topics.sh --create --zookeeper node1:2181,node2:2181,node3:2181 \
 * --replication-factor 3 \
 * --partitions 1 \
 * --topic my-replicated-topic
 *
 * kafka-topics.sh --describe --zookeeper node1:2181
 *
 * kafka-console-producer.sh --broker-list node1:9093,node1:9094,node1:9095 --topic my-replicated-topic
 *
 * kafka-console-consumer.sh --bootstrap-server node1:9093,node1:9094,node1:9095 --from-beginning --topic my-replicated-topic
 */

public class KafkaClientApp {

    public static void main(String[] args) {
        new KafkaProducer(KafkaProperties.TOPIC).start();

        new KafkaConsumer(KafkaProperties.TOPIC).start();

    }

}
