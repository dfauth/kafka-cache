package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.github.dfauth.trycatch.TryCatch.tryCatchSilentlyIgnore;

public class StreamBuilder<K,V> {

    private Map<String, Object> props;
    private String topic;
    private Consumer<ConsumerRecord<K,V>> recordConsumer;
    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;
    private Duration pollingDuration = Duration.ofMillis(50);
    private RebalanceListener<K,V> partitionRevocationListener = consumer -> topicPartitions -> {};
    private RebalanceListener<K,V> partitionAssignmentListener = consumer -> topicPartitions -> {};
    private CommitStrategy.Factory commitStrategy = CommitStrategy.Factory.SYNC;

    public static <V> StreamBuilder<String,V> stringKeyBuilder() {
        return new StreamBuilder().withKeyDeserializer(new StringDeserializer());
    }

    public static <K,V> StreamBuilder<K,V> builder() {
        return new StreamBuilder();
    }

    public StreamBuilder<K,V> withProperties(Map<String, Object>... props) {
        this.props = Arrays.stream(props).reduce(new HashMap<>(), (m1, m2) -> {
            m1.putAll(m2);
            return m1;
        });
        return this;
    }

    public StreamBuilder<K,V> withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public StreamBuilder<K,V> withKeyDeserializer(Deserializer<K> deserializer) {
        this.keyDeserializer = deserializer;
        return this;
    }

    public StreamBuilder<K,V> withValueDeserializer(Deserializer<V> deserializer) {
        this.valueDeserializer = deserializer;
        return this;
    }

    public StreamBuilder<K,V> withRecordConsumer(Consumer<ConsumerRecord<K,V>> recordConsumer) {
        this.recordConsumer = recordConsumer;
        return this;
    }

    public StreamBuilder<K,V> withKeyValueConsumer(BiConsumer<K,V> keyValueConsumer) {
        this.recordConsumer = r -> keyValueConsumer.accept(r.key(), r.value());
        return this;
    }

    public StreamBuilder<K,V> withValueConsumer(Consumer<V> valueConsumer) {
        this.recordConsumer = r -> valueConsumer.accept(r.value());
        return this;
    }

    public StreamBuilder<K,V> withPollingDuration(Duration duration) {
        this.pollingDuration = duration;
        return this;
    }

    public StreamBuilder<K, V> onPartitionAssignment(RebalanceListener<K,V> partitionAssignmentListener) {
        this.partitionAssignmentListener = partitionAssignmentListener;
        return this;
    }

    public StreamBuilder<K, V> onPartitionRevocation(RebalanceListener<K,V> partitionRevocationListener) {
        this.partitionRevocationListener = partitionRevocationListener;
        return this;
    }

    public StreamBuilder<K, V> withCommitStrategy(CommitStrategy.Factory commitStrategy) {
        this.commitStrategy = commitStrategy;
        return this;
    }

    public KafkaStream<K,V> build() {
        return new KafkaStream(this.props, this.topic, this.keyDeserializer, this.valueDeserializer, this.recordConsumer, pollingDuration, partitionAssignmentListener, partitionRevocationListener, commitStrategy);
    }

    public static class KafkaStream<K,V> {

        private final Map<String, Object> props;
        private final String topic;
        private final Deserializer<K> keyDeserializer;
        private final Deserializer<V> valueDeserializer;
        private final Duration duration;
        private final Consumer<ConsumerRecord<K,V>> recordConsumer;
        private final AtomicBoolean isRunning = new AtomicBoolean(false);
        private KafkaConsumer<K,V> consumer;
        private final Duration timeout;
        private final RebalanceListener<K,V> partitionRevocationListener;
        private final RebalanceListener<K,V> partitionAssignmentListener;
        private final CommitStrategy.Factory commitStrategyFactory;
        private CommitStrategy commitStrategy;

        public KafkaStream(Map<String, Object> props, String topic, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Consumer<ConsumerRecord<K,V>> recordConsumer, Duration duration, RebalanceListener<K,V> partitionAssignmentListener, RebalanceListener<K,V> partitionRevocationListener, CommitStrategy.Factory commitStrategy) {
            this(props, topic, keyDeserializer, valueDeserializer, recordConsumer, duration, Duration.ofMillis(1000), partitionAssignmentListener, partitionRevocationListener, commitStrategy);
        }

        public KafkaStream(Map<String, Object> props, String topic, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Consumer<ConsumerRecord<K,V>> recordConsumer, Duration duration, Duration timeout, RebalanceListener<K,V> partitionAssignmentListener, RebalanceListener<K,V> partitionRevocationListener, CommitStrategy.Factory commitStrategy) {
            this.props = props;
            this.topic = topic;
            this.keyDeserializer = keyDeserializer;
            this.valueDeserializer = valueDeserializer;
            this.duration = duration;
            this.recordConsumer = recordConsumer;
            this.timeout = timeout;
            this.partitionAssignmentListener = partitionAssignmentListener;
            this.partitionRevocationListener = partitionRevocationListener;
            this.commitStrategyFactory = commitStrategy;
            this.props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        }

        public void start() {
            isRunning.set(true);
            consumer = new KafkaConsumer(props, keyDeserializer, valueDeserializer);
            commitStrategy = commitStrategyFactory.create(consumer);
            Consumer<Collection<TopicPartition>> x = partitionRevocationListener.withKafkaConsumer(consumer);
            Consumer<Collection<TopicPartition>> y = partitionAssignmentListener.withKafkaConsumer(consumer);
            consumer.subscribe(Collections.singleton(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    x.accept(partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    y.accept(partitions);
                }
            });
            Executors.newSingleThreadExecutor().execute(() -> {
                while(isRunning.get()) {
                    tryCatchSilentlyIgnore(() -> {
                        ConsumerRecords<K, V> records = consumer.poll(duration);
                        records.records(topic).forEach(r -> {
                            recordConsumer.accept(r);
                        });
                        commitStrategy.tryCommit();
                    });
                }
                consumer.close(timeout);
            });
        }

        public void stop() {
            isRunning.set(false);
        }

        public CommitStrategy commitStrategy() {
            return commitStrategy;
        }
    }

}
