package com.github.dfauth.kafka.actor;

import java.util.Collections;
import java.util.Map;

public interface ActorEnvelope<T> {

    static <T> ActorEnvelope<T> withSender(T t, String sender) {
        return of(t, Collections.singletonMap("SENDER", sender));
    }

    static <T> ActorEnvelope<T> of(T t) {
        return of(t, Collections.emptyMap());
    }

    static <T> ActorEnvelope<T> of(T t, Map<String, Object> metadata) {
        return new ActorEnvelope<>() {
            @Override
            public T payload() {
                return t;
            }

            @Override
            public Map<String, Object> metadata() {
                return metadata;
            }
        };
    }

    default Map<String, Object> metadata() {
        return Collections.emptyMap();
    }

    T payload();
}
