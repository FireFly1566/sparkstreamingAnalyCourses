package com.sparkstreaming.kafka;

/**
 * @param
 * @Description: Kafka 常用配置文件
 * @return
 */

public class KafkaProperties {

    public static final String ZK = "node1:2181,node2:2181,node3:2181";

    public static final String TOPIC = "my-replicated-topic";

    public static final String BROKER_LIST = "node1:9093,node1:9094,node1:9095";

    public static final String GROUP_ID = "test_group1";


}
