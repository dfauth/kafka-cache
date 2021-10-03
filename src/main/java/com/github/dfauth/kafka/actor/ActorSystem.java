package com.github.dfauth.kafka.actor;

import com.github.dfauth.avro.Envelope;
import com.github.dfauth.kafka.dispatcher.KafkaDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;

import java.util.function.Consumer;

@Slf4j
public class ActorSystem<T extends SpecificRecord> {

    private final KafkaDispatcher<String, Envelope, String, T> dispatcher;

    public ActorSystem(KafkaDispatcher<String, Envelope, String, T> dispatcher) {
        this.dispatcher = dispatcher;
    }

    public ActorSystem<T> newActor(String key, ActorContextAware<Consumer<T>> actor) {
        ActorContext ctx = () -> key;
        this.dispatcher.handle(key, actor.withActorContext(ctx));
        return this;
    }
}
