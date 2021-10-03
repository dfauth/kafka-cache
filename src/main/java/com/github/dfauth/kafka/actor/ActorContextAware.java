package com.github.dfauth.kafka.actor;

public interface ActorContextAware<T> {
    T withActorContext(ActorContext ctx);
}
