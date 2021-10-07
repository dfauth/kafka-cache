package com.github.dfauth.kafka.utils;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Optional;
import java.util.function.Consumer;

import static com.github.dfauth.trycatch.TryCatch.tryFinally;

public abstract class BaseSubscriber<T> implements Subscriber<T> {

    protected Optional<Subscription> optSubscription = Optional.empty();

    public static <T> Subscriber<T> oneTimeConsumer(Consumer<T> consumer) {
        return new BaseSubscriber<T>() {

            @Override
            public void onNext(T t) {
                tryFinally(() -> consumer.accept(t), () -> optSubscription.ifPresent(s -> s.cancel()));
            }
        };
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        optSubscription = Optional.ofNullable(subscription);
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(T t) {
    }

    @Override
    public void onError(Throwable throwable) {
    }

    @Override
    public void onComplete() {
    }
}
