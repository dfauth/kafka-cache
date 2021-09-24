package com.github.dfauth.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

abstract class CommitStrategy {

    protected KafkaConsumer<?, ?> consumer;

    public CommitStrategy(KafkaConsumer<?, ?> consumer) {
        this.consumer = consumer;
    }

    public final void commit(String topic, int partition, long offset) {
        commit(new TopicPartition(topic, partition), new OffsetAndMetadata(offset));
    }

    public final void commit(ConsumerRecord<?,?> record) {
        commit(record.topic(), record.partition(), record.offset());
    }

    public void commit(TopicPartition tp, OffsetAndMetadata om) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName()+" does not support use of this API - use the correct commit strategy implementation");
    }

    abstract void tryCommit();

    enum Factory {
        SYNC(c -> new SyncCommitStrategy(c)),
        ASYNC(c -> new AsyncCommitStrategy(c)),
        EXTERNAL_SYNC(c -> new ExternalSyncCommitStrategy(c)),
        EXTERNAL_ASYNC(c -> new ExternalAsyncCommitStrategy(c));

        private Function<KafkaConsumer<?, ?>, CommitStrategy> f;

        Factory(Function<KafkaConsumer<?, ?>, CommitStrategy> f) {
            this.f = f;
        }

        public CommitStrategy create(KafkaConsumer<?,?> consumer) {
            return f.apply(consumer);
        }
    }

    private static class SyncCommitStrategy extends CommitStrategy {

        public SyncCommitStrategy(KafkaConsumer<?, ?> consumer) {
            super(consumer);
        }

        @Override
        public void tryCommit() {
            consumer.commitSync();
        }
    }

    @Slf4j
    private static class AsyncCommitStrategy extends CommitStrategy {

        public AsyncCommitStrategy(KafkaConsumer<?, ?> consumer) {
            super(consumer);
        }

        @Override
        public void tryCommit() {
            consumer.commitAsync((o,e) -> Optional.ofNullable(e).ifPresent(_e -> log.error(_e.getMessage(), _e)));
        }
    }

    private static class ExternalSyncCommitStrategy extends CommitStrategy {

        protected Map<TopicPartition, OffsetAndMetadata> records = new ConcurrentHashMap<>();

        public ExternalSyncCommitStrategy(KafkaConsumer<?, ?> consumer) {
            super(consumer);
        }

        public void commit(TopicPartition tp, OffsetAndMetadata om) {
            records.compute(tp, (_tp, _om) -> Optional.ofNullable(_om).filter(_o -> om.offset() > _o.offset()).orElse(om));
        }

        @Override
        public void tryCommit() {
            consumer.commitSync(records);
            records.clear();
        }
    }

    @Slf4j
    private static class ExternalAsyncCommitStrategy extends ExternalSyncCommitStrategy {

        public ExternalAsyncCommitStrategy(KafkaConsumer<?, ?> consumer) {
            super(consumer);
        }

        @Override
        public void tryCommit() {
            consumer.commitAsync(records, (o,e) -> {
                Optional.ofNullable(o).ifPresent(_o -> records.clear());
                Optional.ofNullable(e).ifPresent(_e -> log.error(_e.getMessage(), _e));
            });
        }
    }
}
