package com.github.dfauth.kafka.cache;

import com.github.dfauth.kafka.EmbeddedKafka;
import com.github.dfauth.kafka.KafkaSink;
import com.github.dfauth.kafka.RebalanceListener;
import com.github.dfauth.kafka.StreamBuilder;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class UnitTest {

    private static final Logger logger = LoggerFactory.getLogger(UnitTest.class);

    public static final String TOPIC = "topic";
    public static final String K = "key";
    public static final String V = "value";
    private static final int PARTITIONS = 1;

    @Test
    public void testIt() throws ExecutionException, InterruptedException {
        Cache<String, String> cache = CacheBuilder.newBuilder().build();
        CompletableFuture<String> value = EmbeddedKafka.embeddedKafkaWithTopic(TOPIC)
                .withPartitions(PARTITIONS)
                .withGroupId("blah")
                .runAsyncTest(f -> config -> {
                    StreamBuilder.KafkaStream<String, String> stream = StreamBuilder.<String, String>builder()
                            .withKeyDeserializer(new StringDeserializer())
                            .withValueDeserializer(new StringDeserializer())
                            .withProperties(config)
                            .withTopic(TOPIC)
                            .withKeyValueConsumer((k, v) -> {
                                cache.put(k,v);
                                f.complete(cache.getIfPresent(k));
                            })
                            .onPartitionAssignment(RebalanceListener.seekToBeginning())
                            .build();

                    stream.start();

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
