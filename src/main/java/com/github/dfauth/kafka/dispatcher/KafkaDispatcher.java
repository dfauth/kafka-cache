package com.github.dfauth.kafka.dispatcher;

import com.github.dfauth.kafka.RebalanceListener;
import com.github.dfauth.kafka.StreamBuilder;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static com.github.dfauth.kafka.RebalanceListener.noOp;

public class KafkaDispatcher<K, V, T, R> {

    private final StreamBuilder<K, V> builder;
    private final Cache<T, Subscriber<R>> cache;
    private final BiFunction<K, V, T> keyMapper;
    private final BiFunction<K, V, R> valueMapper;
    private final BiConsumer<T, Subscriber<R>> messageConsumer;
    private StreamBuilder.KafkaStream<K, V> stream;

    public KafkaDispatcher(StreamBuilder<K, V> builder, Cache<T, Subscriber<R>> cache, BiFunction<K, V, T> keyMapper, BiFunction<K, V, R> valueMapper, BiConsumer<T, Subscriber<R>> messageConsumer, RebalanceListener<K,V> partitionAssignmentConsumer, RebalanceListener<K,V> partitionRevocationConsumer) {
        this.builder = builder;
        this.cache = cache;
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
        this.messageConsumer = messageConsumer;
        this.builder.onPartitionAssignment(partitionAssignmentConsumer);
        this.builder.onPartitionRevocation(partitionRevocationConsumer);
    }

    public static <K, V, T, R> Builder<K, V, T, R> builder() {
        return new Builder();
    }

    public static <K, V, R> Builder<K, V, K, R> unmappedKeyBuilder() {
        return new Builder()
                .withKeyMapper((k,v) -> k);
    }

    public static <V, R> Builder<String, V, String, R> unmappedStringKeyBuilder() {
        return new Builder()
                .withKeyMapper((k,v) -> k)
                .withKeyDeserializer(new StringDeserializer());
    }

    public static <K, V, T> Builder<K, V, T, V> unmappedValueBuilder() {
        return new Builder()
                .withValueMapper((k,v) -> v);
    }

    public static <K, V> Builder<K, V, K, V> unmappedBuilder() {
        return new Builder()
                .withKeyMapper((k,v) -> k)
                .withValueMapper((k,v) -> v);
    }

    public void start() {
        this.stream = this.builder.withKeyValueConsumer((k, v) -> {
            T _k = keyMapper.apply(k, v);
            R _v = valueMapper.apply(k, v);
            Optional.ofNullable(cache.getIfPresent(_k)).ifPresent(_s -> _s.onNext(_v));
            this.messageConsumer.accept(_k, cache.getIfPresent(_k));
        }).build();
        this.stream.start();
    }

    public void stop() {
        this.stream.stop();
    }

    public void handle(T t, Consumer<R> consumer) {
        handle(t, new Subscriber<R>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(R r) {
                consumer.accept(r);
            }

            @Override
            public void onError(Throwable throwable) {}

            @Override
            public void onComplete() {}
        });
    }

    public void handle(T t, Subscriber<R> subscriber) {
        this.cache.put(t, subscriber);
        subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long l) {}

            @Override
            public void cancel() {
                cache.invalidate(t);
                subscriber.onComplete();
            }
        });
    }

    public static class Builder<K, V, T, R> {

        private StreamBuilder<K, V> streamBuilder = StreamBuilder.builder();
        private final CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        private BiFunction<K, V, T> keyMapper;
        private BiFunction<K, V, R> valueMapper;
        private RebalanceListener<K,V> partitionAssignmentConsumer = noOp();
        private RebalanceListener<K,V> partitionRevocationConsumer = noOp();
        private BiConsumer<T, Subscriber<R>> messageConsumer = (k,v) -> {};

        public KafkaDispatcher<K, V, T, R> build() {
            return new KafkaDispatcher<>(
                    streamBuilder,
                    cacheBuilder.build(),
                    keyMapper,
                    valueMapper,
                    messageConsumer,
                    partitionAssignmentConsumer,
                    partitionRevocationConsumer
            );
        }

        public KafkaDispatcher.Builder<K, V, T, R> withKeyDeserializer(Deserializer<K> keyDeserializer) {
            streamBuilder.withKeyDeserializer(keyDeserializer);
            return this;
        }

        public KafkaDispatcher.Builder<K, V, T, R> withValueDeserializer(Deserializer<V> valueDeserializer) {
            streamBuilder.withValueDeserializer(valueDeserializer);
            return this;
        }

        public Builder<K, V, T, R> withProperties(Map<String, Object>... configs) {
            streamBuilder.withProperties(configs);
            return this;
        }

        public Builder<K, V, T, R> withTopic(String topic) {
            streamBuilder.withTopic(topic);
            return this;
        }

        public Builder<K, V, T, R> withCacheConfiguration(Consumer<CacheBuilder<Object, Object>> consumer) {
            consumer.accept(cacheBuilder);
            return this;
        }

        public Builder<K, V, T, R> withKeyMapper(BiFunction<K,V,T> keyMapper) {
            this.keyMapper = keyMapper;
            return this;
        }

        public Builder<K, V, T, R> withValueMapper(BiFunction<K,V,R> valueMapper) {
            this.valueMapper = valueMapper;
            return this;
        }

        public Builder<K, V, T, R> onPartitionAssignment(RebalanceListener<K,V> consumer) {
            this.partitionAssignmentConsumer = consumer;
            return this;
        }

        public Builder<K, V, T, R> onPartitionRevocation(RebalanceListener<K,V> consumer) {
            this.partitionRevocationConsumer = consumer;
            return this;
        }

        public Builder<K, V, T, R> onMessage(BiConsumer<T, Subscriber<R>> consumer) {
            this.messageConsumer = consumer;
            return this;
        }
    }
}
