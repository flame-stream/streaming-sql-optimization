package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.sql.agents.impl.StatisticsHandling;
import io.grpc.stub.StreamObserver;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class CoordinatorStatsSourceSinkTest {
    @Test
    public void simpleTest() throws Exception {
        final var targetStreamObserver = new Function<String, StreamObserver<Services.Stats>>() {
            String target;
            Services.Stats stats;

            @Override
            public StreamObserver<Services.Stats> apply(String target) {
                this.target = target;
                return new StreamObserver<>() {
                    @Override
                    public void onNext(Services.Stats value) {
                        stats = value;
                    }

                    @Override
                    public void onError(Throwable t) {
                        t.printStackTrace();
                    }

                    @Override
                    public void onCompleted() {
                    }
                };
            }
        };
        try (
                var ignored = new StatisticsHandling.NIOServer(1111, targetStreamObserver);
                var client = new StatisticsHandling.StatsSender(
                        new InetSocketAddress("localhost", 1111),
                        "my message"
                )
        ) {
            client.send(1, Collections.singletonMap("bid", 1.));
        }
        assertEquals("my message", targetStreamObserver.target);
        assertEquals(
                Services.Stats.newBuilder().setTimestamp(1).putCardinality("bid", 1).build(),
                targetStreamObserver.stats
        );
    }
}
