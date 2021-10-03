package com.github.dfauth.kafka.assertion;

import com.github.dfauth.trycatch.ExceptionalConsumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

public interface Assertions extends Callable<Boolean> {

    static Builder builder() {
        return new Builder();
    }

    default boolean performAssertions() throws Exception {
        return call();
    }

    class Builder {

        private Map<CompletableFuture<?>, Runnable> runnables = Collections.emptyMap();

        public <T> CompletableFuture<T> assertThat(ExceptionalConsumer<CompletableFuture<T>> consumer) {
            CompletableFuture<T> _f = new CompletableFuture<>();
            runnables = new HashMap(runnables);
            runnables.put(_f, () -> {
                consumer.accept(_f);
            });
            return _f;
        }

        public Assertions build(CompletableFuture<Assertions> f) {
            Assertions assertions = () -> {
                runnables.entrySet().forEach(e -> {
                    e.getValue().run();
                });
                return true;
            };
            CompletableFuture.allOf(runnables.keySet().toArray(new CompletableFuture[runnables.keySet().size()])).handle((_null, _t) -> {
                Optional.ofNullable(_t).ifPresentOrElse(
                        _ignore -> f.completeExceptionally(_t),
                        () -> f.complete(assertions)
                );
                return null;
            });
            return assertions;
        }

    }
}
