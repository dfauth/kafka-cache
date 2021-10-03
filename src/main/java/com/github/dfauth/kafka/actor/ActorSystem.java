package com.github.dfauth.kafka.actor;

import com.github.dfauth.avro.Envelope;
import com.github.dfauth.kafka.dispatcher.KafkaDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;

import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public class ActorSystem<T extends SpecificRecord> {

    private final KafkaDispatcher<String, Envelope, String, T> dispatcher;

    public ActorSystem(KafkaDispatcher<String, Envelope, String, T> dispatcher) {
        this.dispatcher = dispatcher;
    }

    public ActorSystem<T> newActor(String key, Function<ActorContext, Consumer<T>> actor) {
        ActorContext ctx = new ActorContext() {
        };
        this.dispatcher.handle(key, actor.apply(ctx));
        return this;
    }
}
