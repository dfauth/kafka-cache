package com.github.dfauth.kafka;

import java.util.function.BiFunction;

public interface TopicPartitionAware<T> extends BiFunction<String, Integer, T> {

    default T apply(String topic, Integer partition) {
        return withTopicPartition(topic, partition);
    }

    T withTopicPartition(String topic, Integer partition);
}
