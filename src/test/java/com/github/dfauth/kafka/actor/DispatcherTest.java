package com.github.dfauth.kafka.actor;

import com.github.dfauth.avro.AvroSerialization;
import com.github.dfauth.avro.Envelope;
import com.github.dfauth.avro.EnvelopeHandler;
import com.github.dfauth.avro.test.TestObject;
import com.github.dfauth.kafka.EmbeddedKafka;
import com.github.dfauth.kafka.KafkaSink;
import com.github.dfauth.kafka.dispatcher.KafkaDispatcher;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.github.dfauth.kafka.RebalanceListener.seekToBeginning;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;

@Slf4j
public class DispatcherTest {

    public static final String TOPIC = "actor-eden";
    public static final String K = "key";
    private static final int PARTITIONS = 1;
    private SchemaRegistryClient schemaRegClient = new MockSchemaRegistryClient();
    private TestObject testObject = TestObject.newBuilder().setKey(1).setValue("1").build();

    @Test
    public void testConsumer() throws ExecutionException, InterruptedException {
        AvroSerialization avroSerialization = new AvroSerialization(schemaRegClient, "dummy", true);
        EnvelopeHandler<TestObject> envelopeHandler = EnvelopeHandler.of(avroSerialization);
        CompletableFuture<TestObject> value = EmbeddedKafka.embeddedKafkaWithTopic(TOPIC)
                .withPartitions(PARTITIONS)
                .withGroupId("god")
                .runAsyncTest(f -> config -> {
                    KafkaDispatcher<String, Envelope, String, TestObject> dispatcher = KafkaDispatcher.<Envelope, TestObject>unmappedStringKeyBuilder()
                            .withValueDeserializer(avroSerialization.envelopeDeserializer())
                            .withValueMapper((k,v) -> envelopeHandler.extractRecord(v))
                            .withProperties(config)
                            .withTopic(TOPIC)
                            .withCacheConfiguration(b -> {})
                            .onPartitionAssignment(seekToBeginning())
                            .build();

                    dispatcher.start();

                    dispatcher.handle(K, v -> {
                        log.info("gotcha {}",v);
                        f.complete(v);
                    });

                    KafkaSink<String, Envelope> sink = KafkaSink.<Envelope>newStringKeyBuilder()
                            .withValueSerializer(avroSerialization.envelopeSerializer())
                            .withProperties(config)
                            .withTopic(TOPIC)
                            .build();
                    RecordMetadata m = sink.publish(K, envelopeHandler.envelope(testObject)).get(1000, TimeUnit.MILLISECONDS);
                    assertNotNull(m);
                });
        assertEquals(testObject, value.get());
    }


    @Test
    public void testSubscriber() throws ExecutionException, InterruptedException {
        AvroSerialization avroSerialization = new AvroSerialization(schemaRegClient, "dummy", true);
        EnvelopeHandler<TestObject> envelopeHandler = EnvelopeHandler.of(avroSerialization);
        CompletableFuture<TestObject> value = EmbeddedKafka.embeddedKafkaWithTopic(TOPIC)
                .withPartitions(PARTITIONS)
                .withGroupId("god")
                .runAsyncTest(f -> config -> {
                    KafkaDispatcher<String, Envelope, String, TestObject> dispatcher = KafkaDispatcher.<Envelope, TestObject>unmappedStringKeyBuilder()
                            .withValueDeserializer(avroSerialization.envelopeDeserializer())
                            .withValueMapper((k,v) -> envelopeHandler.extractRecord(v))
                            .withProperties(config)
                            .withTopic(TOPIC)
                            .withCacheConfiguration(b -> {})
                            .onPartitionAssignment(seekToBeginning())
                            .build();

                    dispatcher.start();

                    dispatcher.handle(K, new Subscriber<>() {
                        @Override
                        public void onSubscribe(Subscription subscription) {
                            subscription.request(Long.MAX_VALUE);
                        }

                        @Override
                        public void onNext(TestObject testObject) {
                            log.info("gotcha {}",testObject);
                            f.complete(testObject);
                        }

                        @Override
                        public void onError(Throwable throwable) {

                        }

                        @Override
                        public void onComplete() {

                        }
                    });

                    KafkaSink<String, Envelope> sink = KafkaSink.<Envelope>newStringKeyBuilder()
                            .withValueSerializer(avroSerialization.envelopeSerializer())
                            .withProperties(config)
                            .withTopic(TOPIC)
                            .build();
                    RecordMetadata m = sink.publish(K, envelopeHandler.envelope(testObject)).get(1000, TimeUnit.MILLISECONDS);
                    assertNotNull(m);
                });
        assertEquals(testObject, value.get());
    }


}
