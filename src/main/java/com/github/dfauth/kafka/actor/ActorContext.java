package com.github.dfauth.kafka.actor;

import com.github.dfauth.kafka.utils.BaseSubscriber;
import org.apache.avro.specific.SpecificRecord;
import org.reactivestreams.Subscriber;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public interface ActorContext {

    String name();

    <T extends SpecificRecord> ActorRef<T> directory();

    default <T extends SpecificRecord> Optional<ActorRef<T>> sender(ActorEnvelope<T> e) {
        return Optional.ofNullable(e.metadata().get("SENDER")).map(v -> actorRef(v.toString()));
    }

    <T extends SpecificRecord> ActorRef<T> actorRef(String key);

    default <T extends SpecificRecord> ActorRef<T> spawn(String name, Consumer<T> consumer) {
        return spawn(name, consumer, Collections.emptyMap());
    }

    default <T extends SpecificRecord> ActorRef<T> spawn(String name, Consumer<T> consumer, Map<String, Object> config) {
        ActorContextAware<Subscriber<T>> subscriber = _ignored -> new BaseSubscriber<>(){
            @Override
            public void onNext(T t) {
                consumer.accept(t);
            }
        };
        return spawn(name, subscriber, config);
    }

    default <T extends SpecificRecord> ActorRef<T> spawn(String name, Subscriber<T> subscriber) {
        return spawn(name, _ignored -> subscriber, Collections.emptyMap());
    }

    default <T extends SpecificRecord> ActorRef<T> spawn(String name, ActorContextAware<Subscriber<T>> subscriber) {
        return spawn(name, subscriber, Collections.emptyMap());
    }

    <T extends SpecificRecord> ActorRef<T> spawn(String name, ActorContextAware<Subscriber<T>> subscriber, Map<String, Object> config);
}
