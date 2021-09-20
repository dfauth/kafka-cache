package com.github.dfauth.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class KafkaSink<K,V> {

    private final KafkaProducer<K, V> producer;
    private final String topic;

    public static <K,V> Builder<K,V> newBuilder() {
        return new Builder();
    }

    public static <V> Builder<String, V> newStringKeyBuilder() {
        return KafkaSink.<String,V>newBuilder().withKeySerializer(new StringSerializer());
    }

    public static <V> Builder<Long, V> newLongKeyBuilder() {
        return KafkaSink.<Long,V>newBuilder().withKeySerializer(new LongSerializer());
    }

    public static <K> Builder<K, String> newStringValueBuilder() {
        return KafkaSink.<K, String>newBuilder().withValueSerializer(new StringSerializer());
    }

    public static Builder<String, String> newStringBuilder() {
        return KafkaSink.<String>newStringKeyBuilder().withValueSerializer(new StringSerializer());
    }

    public KafkaSink(Map<String, Object> props, String topic, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.producer = new KafkaProducer<>(props, keySerializer, valueSerializer);
        this.topic = topic;
    }

    public CompletableFuture<RecordMetadata> publish(K k, V v) {
        CompletableFuture<RecordMetadata> f = new CompletableFuture<>();
        producer.send(new ProducerRecord<>(topic, k, v), (m,e) -> {
            if(e != null) {
                f.completeExceptionally(e);
            } else {
                f.complete(m);
            }
        });
        return f;
    }

    public static class Builder<K,V> {

        private Serializer<K> keySerializer;
        private Serializer<V> valueSerializer;
        private Map<String, Object> props;
        private String topic;

        public KafkaSink<K,V> build() {
            return new KafkaSink<>(props, topic, keySerializer, valueSerializer);
        }

        public Builder<K, V> withKeySerializer(Serializer<K> serializer) {
            this.keySerializer = serializer;
            return this;
        }

        public Builder<K, V> withValueSerializer(Serializer<V> serializer) {
            this.valueSerializer = serializer;
            return this;
        }

        public KafkaSink.Builder<K,V> withProperties(Map<String, Object>... props) {
            this.props = Arrays.stream(props).reduce(new HashMap<>(), (m1, m2) -> {
                m1.putAll(m2);
                return m1;
            });
            return this;
        }

        public KafkaSink.Builder<K,V> withTopic(String topic) {
            this.topic = topic;
            return this;
        }
    }
}
