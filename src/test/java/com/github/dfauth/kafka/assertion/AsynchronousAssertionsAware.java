package com.github.dfauth.kafka.assertion;

import com.github.dfauth.trycatch.ExceptionalConsumer;

import java.util.function.Function;

import static com.github.dfauth.trycatch.TryCatch.tryCatch;

public interface AsynchronousAssertionsAware<T> {

    ExceptionalConsumer<T> withAsynchronousAssertions(AsynchronousAssertions.Builder assertions);

    static <T> Function<T, AsynchronousAssertions> runProvidingAsynchronousAssertions(AsynchronousAssertionsAware<T> aware) {
        AsynchronousAssertions.Builder builder = AsynchronousAssertions.builder();
        return t -> tryCatch(() -> {
            aware.withAsynchronousAssertions(builder).accept(t);
            return builder.build();
        }, e -> builder.build());
    }
}
