package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface KafkaConsumerAware<T,K,V> {
    T withKafkaConsumer(KafkaConsumer<K,V> consumer);
}
