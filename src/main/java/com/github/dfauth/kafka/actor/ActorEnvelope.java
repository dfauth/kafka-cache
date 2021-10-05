package com.github.dfauth.kafka.actor;

import java.util.Collections;
import java.util.Map;

public interface ActorEnvelope<T> {

    static <T> ActorEnvelope<T> withSender(T t, String sender) {
        return of(t, Collections.singletonMap("SENDER", sender));
    }

    static <T> ActorEnvelope<T> of(T t, Map<String, String> metadata) {
        return new ActorEnvelope<T>() {
            @Override
            public T payload() {
                return t;
            }

            @Override
            public Map<String, String> metadata() {
                return metadata;
            }
        };
    }

    default Map<String, String> metadata() {
        return Collections.emptyMap();
    }

    T payload();
}
