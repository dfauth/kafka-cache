package com.github.dfauth.kafka.assertion;

import com.github.dfauth.trycatch.ExceptionalConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface AsynchronousAssertions extends Callable<Boolean> {

    static Builder builder() {
        return new Builder();
    }

    default boolean performAssertions() throws Exception {
        return call();
    }

    CompletableFuture<AsynchronousAssertions> future();

    default boolean performAssertionsWaitingAtMost(int duration, TimeUnit unit) throws Exception {
        return future().get(duration, unit).performAssertions();
    }

    default boolean performAssertionsWaitingAtMost(Duration duration) throws Exception {
        return future().get(duration.toMillis(), TimeUnit.MILLISECONDS).performAssertions();
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

        public AsynchronousAssertions build() {
            CompletableFuture<Void> f = CompletableFuture.allOf(runnables.keySet().toArray(new CompletableFuture[runnables.keySet().size()]));
            AsynchronousAssertions assertions = new AsynchronousAssertions() {
                @Override
                public CompletableFuture<AsynchronousAssertions> future() {
                    return f.thenApply(_null -> this);
                }

                @Override
                public Boolean call() {
                    runnables.entrySet().forEach(e -> {
                        e.getValue().run();
                    });
                    return true;
                }
            };
            return assertions;
        }

    }
}
