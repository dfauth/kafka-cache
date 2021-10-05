package com.github.dfauth.kafka.actor;

import org.apache.avro.specific.SpecificRecord;

import java.util.concurrent.CompletableFuture;

public interface ActorRef<T extends SpecificRecord> {
    void tell(T t);
    <R extends SpecificRecord> CompletableFuture<R> ask(T t);
    String name();
}
