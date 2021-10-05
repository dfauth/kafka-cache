package com.github.dfauth.kafka.actor;

public interface ActorInvocation<T> {

    ActorContext context();
    T message();

    static <T> ActorInvocation<T> of(ActorContext ctx, ActorEnvelope<T> t) {
        return new ActorInvocation<T>() {
            @Override
            public ActorContext context() {
                return ctx;
            }

            @Override
            public T message() {
                return t.payload();
            }
        };
    }
}
