package com.github.dfauth.kafka.actor;

import org.apache.avro.specific.SpecificRecord;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public interface ActorContext {

    String name();

    <T extends SpecificRecord> ActorRef<T> directory();

    default <T extends SpecificRecord> Optional<ActorRef<T>> sender(ActorEnvelope<T> e) {
        return Optional.ofNullable(e.metadata().get("SENDER")).map(v -> actorRef(v));
    }

    <T extends SpecificRecord> ActorRef<T> actorRef(String key);

    default <T extends SpecificRecord> ActorRef<T> spawn(String name, Consumer<T> consumer) {
        return spawn(name, consumer, Collections.emptyMap());
    }

    <T extends SpecificRecord> ActorRef<T> spawn(String name, Consumer<T> consumer, Map<String, Object> config);
}
