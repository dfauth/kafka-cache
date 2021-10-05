package com.github.dfauth.kafka.utils;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.github.dfauth.trycatch.TryCatch.loggingOperator;

public class CompletableFutureUtils {

    public static <R> Function<Throwable, R> loggingFunction() {
        return loggingFunction(null);
    }

    public static <R> Function<Throwable, R> loggingFunction(R defaultValue) {
        return t -> {
            loggingOperator.apply(t);
            return defaultValue;
        };
    }

    public static <T> BiFunction<T, Throwable, Void> asConsumerHandler(Consumer<T> consumer) {
        return asHandlers(t -> {
            consumer.accept(t);
            return null;
        }, loggingFunction());
    }

    public static <T, R> BiFunction<T, Throwable, R> asHandler(Function<T, R> mapper) {
        return asHandlers(mapper, loggingFunction());
    }

    public static <T, R> BiFunction<T, Throwable, R> asHandlers(Function<T, R> mapper, Function<Throwable, R> handler) {
        return (t, e) -> Optional.ofNullable(e).map(handler).orElseGet(() -> mapper.apply(t));
    }
}
