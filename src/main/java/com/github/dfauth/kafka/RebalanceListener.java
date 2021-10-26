package com.github.dfauth.kafka;

import org.apache.kafka.common.TopicPartition;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.github.dfauth.trycatch.TryCatchBuilder.tryCatch;
import static java.util.function.Function.identity;

public interface RebalanceListener<K,V> extends KafkaConsumerAware<Consumer<Collection<TopicPartition>>, K,V>{

    default RebalanceListener<K,V> compose(RebalanceListener<K,V> nested) {
        return c -> tps -> {
            tryCatch(() -> nested.withKafkaConsumer(c).accept(tps))
            .ignoreSilently()
            .andFinallyRun(() -> withKafkaConsumer(c).accept(tps));
        };
    }

    default RebalanceListener<K,V> andThen(RebalanceListener<K,V> following) {
        return following.compose(this);
    }

    static <K,V> RebalanceListener<K,V> offsetsFuture(Consumer<Map<TopicPartition,Long>> consumer) {
        return c -> tps ->
            consumer.accept(tps.stream().collect(Collectors.toMap(identity(), c::position)));
    }

    static <K,V> RebalanceListener<K,V> seekToBeginning() {
        return c -> tps -> c.seekToBeginning(tps);
    }

    static <K,V> RebalanceListener<K,V> seekToEnd() {
        return c -> tps -> c.seekToEnd(tps);
    }

    static <K,V> RebalanceListener<K,V> seekToTimestamp(ZonedDateTime t) {
        return c -> tps ->
                c.offsetsForTimes(tps.stream().collect(Collectors.toMap(identity(), ignored -> t.toInstant().toEpochMilli())))
                        .forEach((tp,o) -> Optional.ofNullable(o).map(_o -> o.offset()).ifPresent(_o -> c.seek(tp,_o)));
    }

    static <K,V> RebalanceListener<K,V> seekToOffsets(Map<TopicPartition, Long> offsets) {
        return c -> tps -> offsets.entrySet().stream().filter(e -> tps.contains(e.getKey())).forEach(e -> c.seek(e.getKey(), e.getValue()));
    }

    static <K,V> RebalanceListener<K,V> topicPartitionListener(Consumer<Collection<TopicPartition>> consumer) {
        return c -> tps ->
            consumer.accept(tps);
    }

    static <K,V> RebalanceListener<K, V> noOp( ){
        return c -> tps -> {};
    }
}
