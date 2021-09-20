package com.github.dfauth.kafka.cache;

import com.github.dfauth.avro.AvroSerialization;
import com.github.dfauth.avro.Envelope;
import com.github.dfauth.avro.EnvelopeHandler;
import com.github.dfauth.avro.test.TestObject;
import com.github.dfauth.kafka.KafkaSink;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.github.dfauth.kafka.EmbeddedKafka.embeddedKafkaWithTopic;
import static com.github.dfauth.kafka.RebalanceListener.seekToBeginning;
import static com.github.dfauth.kafka.cache.TestUtils.ignoringFunction;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AvroCacheTest {

    private static final Logger logger = LoggerFactory.getLogger(AvroCacheTest.class);

    public static final String TOPIC = "topic";
    private static final int PARTITIONS = 1;
    private SchemaRegistryClient schemaRegClient = new MockSchemaRegistryClient();
    private TestObject testObject = TestObject.newBuilder().setKey(1).setValue("1").build();

    @Test
    public void testIt() {
        CountDownLatch latch = new CountDownLatch(1);
        AvroSerialization avroSerialization = new AvroSerialization(schemaRegClient, "dummy", true);
        EnvelopeHandler<TestObject> envelopeHandler = EnvelopeHandler.of(avroSerialization);
        TestObject value = embeddedKafkaWithTopic(TOPIC)
                .withPartitions(PARTITIONS)
                .withGroupId("blah")
                .runTest(ignoringFunction(config -> {
                    KafkaCache<Long, Envelope, Long, TestObject> cache = KafkaCache.<Envelope, TestObject>unmappedLongKeyBuilder()
                            .withValueDeserializer(avroSerialization.envelopeDeserializer())
                            .withValueMapper((k,v) -> envelopeHandler.extractRecord(v))
                            .withProperties(config)
                            .withTopic(TOPIC)
                            .withCacheConfiguration(b -> {})
                            .onPartitionAssignment(seekToBeginning())
                            .onMessage((k,v) -> latch.countDown())
                            .build();

                    cache.start();


                    Envelope envelope = envelopeHandler.envelope(testObject);
                    KafkaSink<Long, Envelope> sink = KafkaSink.<Envelope>newLongKeyBuilder()
                            .withProperties(config)
                            .withTopic(TOPIC)
                            .withValueSerializer(avroSerialization.envelopeSerializer())
                            .build();
                    RecordMetadata m = sink.publish(testObject.getKey(), envelope).get(1000, TimeUnit.MILLISECONDS);
                    assertNotNull(m);
                    latch.await(1000, TimeUnit.MILLISECONDS);
                    return cache.getOptional(testObject.getKey()).get();
                }));
        assertEquals(testObject, value);
    }

}
