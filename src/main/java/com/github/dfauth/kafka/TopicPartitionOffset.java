package com.github.dfauth.kafka;

import com.github.dfauth.trycatch.Try;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.github.dfauth.trycatch.DespatchHandler.Function.identity;

public class TopicPartitionOffset {

    public static Function<Map<TopicPartition,Long>, BiFunction<TopicPartition, Long, Try<Boolean>>> isRecovering = map -> (tp, o) -> Optional.ofNullable(map.get(tp)).<Try<Boolean>>map(_o -> Try.success(o < _o)).orElse(Try.failure(new IllegalArgumentException(tp+" is not an assigned partition")));

    public static BiFunction<TopicPartition, Long, Try<Boolean>> isReplay(Map<TopicPartition,Long> offsets) {
        return isRecovering.apply(offsets);
    }

    public static BiFunction<String, Integer, Function<Long, Try<Boolean>>> isReplay2(Map<TopicPartition,Long> offsets) {
        return (t, p) -> o -> isRecovering.apply(offsets).apply(new TopicPartition(t,p),o);
    }

    public static TopicPartitionAware<ReplayMonitor> replayMonitor(Map<TopicPartition,Long> offsets) {
        return (t, p) -> o -> isRecovering.apply(offsets).apply(new TopicPartition(t,p),o).despatch(identity());
    }
}
