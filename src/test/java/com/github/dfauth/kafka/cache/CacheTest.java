package com.github.dfauth.kafka.cache;

import com.github.dfauth.kafka.KafkaSink;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.github.dfauth.kafka.EmbeddedKafka.embeddedKafkaWithTopic;
import static com.github.dfauth.kafka.RebalanceListener.seekToBeginning;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CacheTest {

    private static final Logger logger = LoggerFactory.getLogger(CacheTest.class);

    public static final String TOPIC = "topic";
    public static final String K = "key";
    public static final String V = "value";
    private static final int PARTITIONS = 1;

    @Test
    public void testIt() throws ExecutionException, InterruptedException {
        CompletableFuture<String> value = embeddedKafkaWithTopic(TOPIC)
                .withPartitions(PARTITIONS)
                .withGroupId("blah")
                .runAsyncTest(f -> config -> {
                    KafkaCache<String, String, String, String> cache = KafkaCache.<String, String>unmappedBuilder()
                            .withKeyDeserializer(new StringDeserializer())
                            .withValueDeserializer(new StringDeserializer())
                            .withProperties(config)
                            .withTopic(TOPIC)
                            .withCacheConfiguration(b -> {})
                            .onPartitionAssignment(seekToBeginning())
                            .onMessage((k,v) -> f.complete(v))
                            .build();

                    cache.start();

                    KafkaSink<String, String> sink = KafkaSink.newStringBuilder()
                            .withProperties(config)
                            .withTopic(TOPIC)
                            .build();
                    RecordMetadata m = sink.publish(K, V).get(1000, TimeUnit.MILLISECONDS);
                    assertNotNull(m);
                });
        assertEquals(V, value.get());
    }

}
