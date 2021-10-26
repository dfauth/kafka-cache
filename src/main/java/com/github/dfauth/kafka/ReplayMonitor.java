package com.github.dfauth.kafka;

import java.util.function.Predicate;

public interface ReplayMonitor extends Predicate<Long> {
    @Override
    default boolean test(Long offset) {
        return isReplay(offset);
    }

    boolean isReplay(long offset);
}
