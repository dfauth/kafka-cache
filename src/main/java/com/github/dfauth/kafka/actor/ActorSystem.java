package com.github.dfauth.kafka.actor;

import com.github.dfauth.avro.AvroSerialization;
import com.github.dfauth.avro.Envelope;
import com.github.dfauth.avro.EnvelopeHandler;
import com.github.dfauth.avro.actor.DirectoryRequest;
import com.github.dfauth.avro.actor.DirectoryResponse;
import com.github.dfauth.kafka.KafkaSink;
import com.github.dfauth.kafka.RebalanceListener;
import com.github.dfauth.kafka.ReplayMonitor;
import com.github.dfauth.kafka.TopicPartitionAware;
import com.github.dfauth.kafka.dispatcher.KafkaDispatcher;
import com.github.dfauth.kafka.utils.BaseSubscriber;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.dfauth.avro.EnvelopeHandler.recast;
import static com.github.dfauth.kafka.RebalanceListener.seekToBeginning;
import static com.github.dfauth.kafka.TopicPartitionOffset.replayMonitor;
import static com.github.dfauth.kafka.utils.BaseSubscriber.oneTimeConsumer;
import static com.github.dfauth.trycatch.TryCatchBuilder.tryCatch;
import static java.util.concurrent.CompletableFuture.completedFuture;

@Slf4j
public class ActorSystem<T extends SpecificRecord> {

    private final KafkaDispatcher<String, Envelope, String, ActorEnvelope<SpecificRecord>> dispatcher;
    private final KafkaSink<String, Envelope> sink;
    private final EnvelopeHandler<T> envelopeHandler;
    private final CompletableFuture<TopicPartitionAware<ReplayMonitor>> f = new CompletableFuture<>();
    private TopicPartitionAware<ReplayMonitor> fn = null;

    public ActorSystem(Map<String, Object> config, AvroSerialization avroSerialization, String topic) {
        this.envelopeHandler = EnvelopeHandler.of(avroSerialization);

        CompletableFuture<TopicPartitionAware<ReplayMonitor>> f = new CompletableFuture<>();

        this.dispatcher = KafkaDispatcher.<Envelope, ActorEnvelope<SpecificRecord>>unmappedStringKeyBuilder()
                .withValueDeserializer(avroSerialization.envelopeDeserializer())
                .withRecordMapper(r -> envelopeHandler.extractRecord(r.value(), (t,h) -> ActorEnvelope.of(t, Stream.<Map.Entry<String, Object>>concat(h.entrySet().stream(), Map.<String, Object>of("topic", r.topic(), "partition", r.partition(), "offset", r.offset()).entrySet().stream()).collect(Collectors.<Map.Entry<String, Object>,String, Object>toMap(e -> e.getKey(), e -> e.getValue())))))
                .withProperties(config)
                .withTopic(topic)
                .withCacheConfiguration(b -> {})
                .onPartitionAssignment(RebalanceListener.<String,Envelope>offsetsFuture(tpm -> f.complete(replayMonitor(tpm))).andThen(seekToBeginning()))
                .build();

        dispatcher.start();

        // block on the future that will return when partitions are assigned
        fn = tryCatch(() -> {
            return f.get(1000, TimeUnit.MILLISECONDS);
        }).reThrowAndRun();

        this.sink = KafkaSink.<Envelope>newStringKeyBuilder()
                .withValueSerializer(avroSerialization.envelopeSerializer())
                .withProperties(config)
                .withTopic(topic)
                .build();
    }

    public ActorSystem<T> newActor(String key, ActorContextAware<Consumer<ActorEnvelope<SpecificRecord>>> actor) {
        ActorContext ctx = new ActorContext() {
            @Override
            public String name() {
                return key;
            }

            @Override
            public boolean isRecovery(ActorEnvelope e) {
                return fn.withTopicPartition((String) e.metadata().get("topic"), (int)e.metadata().get("partition")).isReplay((long)e.metadata().get("offset"));
            }

            @Override
            public ActorRef<DirectoryRequest> directory() {
                return new ActorRef<>() {
                    @Override
                    public void tell(DirectoryRequest r) {
                        throw new UnsupportedOperationException("directory can only respond, you must use ask");
                    }

                    @Override
                    public String name() {
                        return "directory";
                    }

                    @Override
                    public CompletableFuture<DirectoryResponse> ask(DirectoryRequest r) {
                        // currently only support lookup by name
                        return completedFuture(DirectoryResponse.newBuilder().setName(r.getName()).build());
                    }
                };
            }

            @Override
            public <R extends SpecificRecord> ActorRef<R> actorRef(String key) {
                ActorContext ctx = this;
                return new ActorRef<>() {
                    @Override
                    public void tell(R r) {
                        sink.publish(key, recast(envelopeHandler).envelope(r, Collections.singletonMap("SENDER",ctx.name())));
                    }

                    @Override
                    public <U extends SpecificRecord> CompletableFuture<U> ask(R r) {
                        String newKey = anonymise(ctx.name());
                        CompletableFuture<U> f = new CompletableFuture<>();
                        ActorRef<U> tmp = spawn(newKey, oneTimeConsumer(x -> f.complete(x)));
                        sink.publish(key, recast(envelopeHandler).envelope(r, Collections.singletonMap("SENDER", tmp.name())));
                        return f;
                    }

                    @Override
                    public String name() {
                        return key;
                    }
                };
            }

            @Override
            public <R extends SpecificRecord> ActorRef<R> spawn(String name, ActorContextAware<Subscriber<R>> subscriber, Map<String, Object> config) {
                Subscriber<R> s = subscriber.withActorContext(this);
                Subscriber<ActorEnvelope<SpecificRecord>> x = new BaseSubscriber<>() {

                    @Override
                    public void onSubscribe(Subscription subscription) {
                        super.onSubscribe(subscription);
                        s.onSubscribe(subscription);
                    }

                    @Override
                    public void onNext(ActorEnvelope<SpecificRecord> t) {
                        s.onNext((R) t.payload());
                    }
                };

                ActorSystem.this.dispatcher.handle(name, x);
                return new ActorRef<>() {
                    @Override
                    public void tell(R r) {
                        throw new UnsupportedOperationException("anonymous actor cannot be sent message using this actor ref");
                    }

                    @Override
                    public <U extends SpecificRecord> CompletableFuture<U> ask(R r) {
                        throw new UnsupportedOperationException("anonymous actor cannot be sent message using this actor ref");
                    }

                    @Override
                    public String name() {
                        return name;
                    }
                };
            }
        };
        Consumer<ActorEnvelope<SpecificRecord>> x = actor.withActorContext(ctx);
        this.dispatcher.handle(key, x);
        return this;
    }

    private String anonymise(String key) {
        return String.format("%s/%s",key,UUID.randomUUID().toString().substring(16));
    }
}
