package com.github.dfauth.kafka;

import com.google.common.collect.ImmutableMap;
import junit.framework.TestSuite;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.core.BrokerAddress;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.dfauth.trycatch.TryCatch.tryCatch;
import static com.github.dfauth.trycatch.TryCatch.tryCatchIgnore;

public class EmbeddedKafka {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedKafka.class);
//    public <T> T runTest(Function<Map<String, Object>, T> f) {
//        EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker(partitions, false, topic);
//        try {
//            broker.restart(0);
//            return f.apply(Collections.singletonMap(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,Stream.of(broker.getBrokerAddresses()).map(ba -> ba.toString()).collect(Collectors.joining(","))));
//        } catch (Exception e) {
//            logger.error(e.getMessage(), e);
//            throw new RuntimeException(e);
//        } finally {
//            broker.destroy();
//        }
//    }

    public static EmbeddedKafkaRunner embeddedKafkaWithTopic(String topic) {
        return withEmbeddedKafka(topic, Collections.emptyMap());
    }

    public static EmbeddedKafkaRunner withEmbeddedKafka(String topic, Map<String, Object> config) {
        return new EmbeddedKafkaRunner(topic, config);
    }

    public static class EmbeddedKafkaRunner {

        private final String topic;
        private Map<String, Object> config;
        private int partitions;

        public EmbeddedKafkaRunner(String topic, Map<String, Object> config) {
            this(topic, config, 1);
        }

        public EmbeddedKafkaRunner(String topic, Map<String, Object> config, int partitions) {
            this.topic = topic;
            this.config = config;
            this.partitions = partitions;
        }

        public EmbeddedKafkaRunner withGroupId(String groupId) {
            Map<String, Object> tmp = new HashMap(config);
            tmp.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            this.config = tmp;
            return this;
        }

        public EmbeddedKafkaRunner withPartitions(int partitions) {
            this.partitions = partitions;
            return this;
        }

        public void runTest(Consumer<Map<String, Object>> consumer) {
            runTestFuture(p -> tryCatch(() -> {
                consumer.accept(p);
                return CompletableFuture.completedFuture(null);
            }));
        }

        public <T> T runTest(Function<Map<String, Object>, T> f) {
            EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker(1, true, partitions, topic);
            broker.afterPropertiesSet();
            Map<String, Object> p = new HashMap(this.config);
            p.putAll(ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()));
            try {
                return f.apply(p);
            } finally {
                terminate(broker);
            }
        }

        public <T> CompletableFuture<T> runTestFuture(Function<Map<String, Object>, CompletableFuture<T>> f) {
            EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker(1, true, partitions, topic);
            broker.afterPropertiesSet();
            Map<String, Object> p = new HashMap(this.config);
            p.putAll(ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()));
            CompletableFuture<T> _f = f.apply(p);
            return _f.handle((r,e) -> {
                terminate(broker);
                return r;
            });
        }
    }

    private static void terminate(EmbeddedKafkaBroker broker) {
        tryCatchIgnore(() ->
                broker.destroy()
        );
    }
}
