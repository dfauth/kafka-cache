package com.github.dfauth.kafka.cache;

import java.util.function.Function;

import static com.github.dfauth.trycatch.TryCatch.tryCatch;

public class TestUtils {

    public static <T,R> Function<T,R> ignoringFunction(ExceptionalFunction<T,R> f) {
        return t -> tryCatch(() -> f.apply(t), _t -> null);
    }

    interface ExceptionalFunction<T,R> {
        R apply(T t) throws Exception;
    }
}
