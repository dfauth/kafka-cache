package com.github.dfauth.kafka.actor;

import com.github.dfauth.avro.AvroSerialization;
import com.github.dfauth.avro.Envelope;
import com.github.dfauth.avro.EnvelopeHandler;
import com.github.dfauth.avro.test.TestObject;
import com.github.dfauth.kafka.EmbeddedKafka;
import com.github.dfauth.kafka.KafkaSink;
import com.github.dfauth.kafka.assertion.Assertions;
import com.github.dfauth.kafka.assertion.AsynchronousAssertions;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.github.dfauth.kafka.utils.CompletableFutureUtils.asConsumerHandler;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ActorTest {

    public static final String TOPIC = "actor-eden";
    public static final String KEY1 = "actorKey1";
    public static final String KEY2 = "actorKey2";
    private static final int PARTITIONS = 1;
    private SchemaRegistryClient schemaRegClient = new MockSchemaRegistryClient();
    private TestObject testObject1 = TestObject.newBuilder().setKey(1).setValue("1").build();
    private TestObject testObject2 = TestObject.newBuilder().setKey(2).setValue("2").build();

    @Test
    public void testConsumer1() throws Exception {

        AvroSerialization avroSerialization = new AvroSerialization(schemaRegClient, "dummy", true);
        EnvelopeHandler<TestObject> envelopeHandler = EnvelopeHandler.of(avroSerialization);
        CompletableFuture<Assertions> value = EmbeddedKafka.embeddedKafkaWithTopic(TOPIC)
                .withPartitions(PARTITIONS)
                .withGroupId("god")
                .runAsyncTest(f -> config -> {
                    Assertions.Builder assertions = Assertions.builder();
                    CompletableFuture<TestObject> f1 = assertions.assertThat(_f -> assertEquals(testObject1, _f.get()));
                    CompletableFuture<TestObject> f2 = assertions.assertThat(_f -> assertEquals(testObject2, _f.get()));
                    assertions.build(f);

                    KafkaSink<String, Envelope> sink = KafkaSink.<Envelope>newStringKeyBuilder()
                            .withValueSerializer(avroSerialization.envelopeSerializer())
                            .withProperties(config)
                            .withTopic(TOPIC)
                            .build();
                    ActorSystem<TestObject> actorSystem = new ActorSystem(config, avroSerialization, TOPIC);

                    actorSystem.newActor(KEY1, ctx -> v -> {
                        log.info("gotcha {} {}",KEY1, v);
                        f1.complete((TestObject) v.payload());
                    });

                    actorSystem.newActor(KEY2, ctx -> v -> {
                        log.info("gotcha {} {}",KEY2, v);
                        f2.complete((TestObject) v.payload());
                    });

                    assertNotNull(sink.publish(KEY1, envelopeHandler.envelope(testObject1)).get(1000, TimeUnit.MILLISECONDS));
                    assertNotNull(sink.publish(KEY2, envelopeHandler.envelope(testObject2)).get(1000, TimeUnit.MILLISECONDS));
                });
        assertTrue(value.get(7000, TimeUnit.MILLISECONDS).performAssertions());
    }


    @Test
    public void testConsumer2() throws Exception {

        AvroSerialization avroSerialization = new AvroSerialization(schemaRegClient, "dummy", true);
        EnvelopeHandler<TestObject> envelopeHandler = EnvelopeHandler.of(avroSerialization);
        AsynchronousAssertions assertions = EmbeddedKafka.embeddedKafkaWithTopic(TOPIC)
                .withPartitions(PARTITIONS)
                .withGroupId("god")
                .runWithAssertions(assertionsBuilder -> config -> {

                    KafkaSink<String, Envelope> sink = KafkaSink.<Envelope>newStringKeyBuilder()
                            .withValueSerializer(avroSerialization.envelopeSerializer())
                            .withProperties(config)
                            .withTopic(TOPIC)
                            .build();

                    ActorSystem<TestObject> actorSystem = new ActorSystem(config, avroSerialization, TOPIC);

                    CompletableFuture<ActorInvocation<TestObject>> f1 = assertionsBuilder.assertThat(_f -> {
                        assertEquals(testObject1, _f.get().message());
                        assertEquals(KEY1, _f.get().context().name());
                    });
                    actorSystem.newActor(KEY1, ctx -> v -> {
                        log.info("gotcha {} {}",KEY1, v);
                        f1.complete(ActorInvocation.of(ctx, ActorEnvelope.of((TestObject) v.payload(), v.metadata())));
                    });

                    CompletableFuture<ActorInvocation<TestObject>> f2 = assertionsBuilder.assertThat(_f -> {
                        assertEquals(testObject2, _f.get().message());
                        assertEquals(KEY2, _f.get().context().name());
                    });
                    actorSystem.newActor(KEY2, ctx -> v -> {
                        log.info("gotcha {} {}",KEY2, v);
                        f2.complete(ActorInvocation.of(ctx, ActorEnvelope.of((TestObject) v.payload(), v.metadata())));
                    });

                    assertNotNull(sink.publish(KEY1, envelopeHandler.envelope(testObject1)).get(1000, TimeUnit.MILLISECONDS));
                    assertNotNull(sink.publish(KEY2, envelopeHandler.envelope(testObject2)).get(1000, TimeUnit.MILLISECONDS));
                });
        assertTrue(assertions.performAssertionsWaitingAtMost(Duration.ofMillis(7000)));
    }

    @Test
    public void testAsk() throws Exception {

        AvroSerialization avroSerialization = new AvroSerialization(schemaRegClient, "dummy", true);
        EnvelopeHandler<TestObject> envelopeHandler = EnvelopeHandler.of(avroSerialization);
        AsynchronousAssertions assertions = EmbeddedKafka.embeddedKafkaWithTopic(TOPIC)
                .withPartitions(PARTITIONS)
                .withGroupId("god")
                .runWithAssertions(assertionsBuilder -> config -> {

                    KafkaSink<String, Envelope> sink = KafkaSink.<Envelope>newStringKeyBuilder()
                            .withValueSerializer(avroSerialization.envelopeSerializer())
                            .withProperties(config)
                            .withTopic(TOPIC)
                            .build();

                    ActorSystem<TestObject> actorSystem = new ActorSystem(config, avroSerialization, TOPIC);

                    CompletableFuture<ActorInvocation<TestObject>> f1 = assertionsBuilder.assertThat(_f -> {
                        assertEquals(testObject1, _f.get().message());
                        assertEquals(KEY1, _f.get().context().name());
                    });
                    CompletableFuture<TestObject> f3 = assertionsBuilder.assertThat(_f -> {
                        assertEquals(testObject2, _f.get());
                    });
                    actorSystem.newActor(KEY1, ctx -> v -> {
                        log.info("gotcha {} {}",KEY1, v);
                        f1.complete(ActorInvocation.of(ctx, ActorEnvelope.of((TestObject) v.payload(), v.metadata())));
                        if(testObject1.equals(v.payload())) {
                            ctx.actorRef(KEY2).<TestObject>ask(testObject1)
                                    .handle(asConsumerHandler(o ->
                                            f3.complete(o)
                                    ));
                        }
                    });

                    actorSystem.newActor(KEY2, ctx -> v -> {
                        if(testObject1.equals(v.payload())) {
                            ctx.sender(v).ifPresent(ref -> ref.tell(testObject2));
                        }
                    });

                    assertNotNull(sink.publish(KEY1, envelopeHandler.envelope(testObject1)).get(1000, TimeUnit.MILLISECONDS));
                    assertNotNull(sink.publish(KEY2, envelopeHandler.envelope(testObject2)).get(1000, TimeUnit.MILLISECONDS));
                });
        assertTrue(assertions.performAssertionsWaitingAtMost(Duration.ofMillis(7000)));
    }

}
