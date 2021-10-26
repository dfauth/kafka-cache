package com.github.dfauth.kafka;

import com.github.dfauth.trycatch.ExceptionalConsumer;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.github.dfauth.trycatch.TryCatch.tryCatch;

public interface CompletableFutureAware<T,R> {

    ExceptionalConsumer<R> withCompletableFuture(CompletableFuture<T> f);

    static <T,R> Function<R, CompletableFuture<T>> runProvidingFuture(CompletableFutureAware<T,R> fAware) {
        CompletableFuture<T> f = new CompletableFuture<>();
        return r -> tryCatch(() -> {
            fAware.withCompletableFuture(f).accept(r);
            return f;
        }, e -> {
            if(!f.isCompletedExceptionally()) {
                f.completeExceptionally(e);
            }
            return f;
        });
    }
}