package com.github.dfauth.kafka.cache;

import com.github.dfauth.kafka.EmbeddedKafka;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.github.dfauth.kafka.KafkaSink;
import com.github.dfauth.kafka.StreamBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.github.dfauth.kafka.cache.TestUtils.ignoringFunction;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class UnitTest {

    private static final Logger logger = LoggerFactory.getLogger(UnitTest.class);

    public static final String TOPIC = "topic";
    public static final String K = "key";
    public static final String V = "value";
    private static final int PARTITIONS = 1;

    @Test
    public void testIt() {
        CountDownLatch partitionAssignmentLatch = new CountDownLatch(PARTITIONS);
        CountDownLatch messageLatch = new CountDownLatch(1);
        Cache<String, String> cache = CacheBuilder.newBuilder().build();
        String value = EmbeddedKafka.embeddedKafkaWithTopic(TOPIC)
                .withPartitions(PARTITIONS)
                .withGroupId("blah")
                .runTest(ignoringFunction(config -> {
                    StreamBuilder.KafkaStream<String, String> stream = StreamBuilder.<String, String>builder()
                            .withKeyDeserializer(new StringDeserializer())
                            .withValueDeserializer(new StringDeserializer())
                            .withProperties(config, Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
                            .withTopic(TOPIC)
                            .withRecordConsumer((k, v) -> {
                                cache.put(k,v);
                                messageLatch.countDown();
                            })
                            .onPartitionAssignment(c -> tp -> tp.forEach(ignored -> partitionAssignmentLatch.countDown()))
                            .build();

                    stream.start();

                    partitionAssignmentLatch.await();
                    KafkaSink<String, String> sink = KafkaSink.newStringBuilder()
                            .withProperties(config)
                            .withTopic(TOPIC)
                            .build();
                    RecordMetadata m = sink.publish(K, V).get(1000, TimeUnit.MILLISECONDS);
                    assertNotNull(m);
                    messageLatch.await();
                    return cache.getIfPresent(K);
                }));
        assertEquals(V, value);
    }

}
