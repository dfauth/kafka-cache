package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.github.dfauth.kafka.RebalanceListener.offsetsFuture;
import static com.github.dfauth.kafka.TopicPartitionOffset.replayMonitor;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class RebalanceListenerTest {

    private static final Logger logger = LoggerFactory.getLogger(RebalanceListenerTest.class);
    private static final String T = "TOPIC";
    private static final int P = 0;

    @Test
    public void testIt() throws ExecutionException, InterruptedException {

        TopicPartition tp = new TopicPartition(T, P);
        long o = 1L;

        CompletableFuture<TopicPartitionAware<ReplayMonitor>> f = new CompletableFuture<>();
        RebalanceListener<String, String> rebalanceListener = RebalanceListener.<String,String>seekToBeginning().compose(offsetsFuture(tpm -> f.complete(replayMonitor(tpm))));
//        BiFunction<String, Integer, CompletableFuture<Boolean>> f1 = (t, p) -> f.handle(asHandler(_f -> _f.apply(t, p).apply(o-1).despatch(identity())));
        KafkaConsumer<String, String> c = mock(KafkaConsumer.class);
        when(c.position(tp)).thenReturn(o);
        rebalanceListener.withKafkaConsumer(c).accept(Collections.singleton(tp));
        verify(c, times(1)).seekToBeginning(Collections.singleton(tp));
        assertTrue(f.get().withTopicPartition(tp.topic(), tp.partition()).isReplay(o-1));
        assertFalse(f.get().withTopicPartition(tp.topic(), tp.partition()).isReplay(o));
    }
}
