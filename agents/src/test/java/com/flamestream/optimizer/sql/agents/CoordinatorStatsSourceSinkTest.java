package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.sql.agents.impl.CoordinatorImpl;
import com.flamestream.optimizer.sql.agents.impl.CostEstimatorImpl;
import com.flamestream.optimizer.sql.agents.impl.ExecutorImpl;
import com.flamestream.optimizer.sql.agents.impl.StatisticsHandling;
import com.flamestream.optimizer.sql.agents.testutils.TestPipelineOptions;
import org.junit.Test;

import java.net.InetSocketAddress;
import static org.junit.Assert.assertEquals;

public class CoordinatorStatsSourceSinkTest {
    @Test
    public void simpleTest() throws InterruptedException {
        var executor = new ExecutorImpl(new TestPipelineOptions());
        var server = new StatisticsHandling.NIOServer(
                new CoordinatorImpl(
                        new CostEstimatorImpl(),
                        executor
                ),
                1111
        );
        var client = new StatisticsHandling.MessageSender(
                "my message",
                new InetSocketAddress("localhost", 1111)
        );

        var serverThread = new Thread(server);
        serverThread.start();

        Thread.sleep(100);
        client.send();
        Thread.sleep(100);
        assertEquals("my message", server.result);

    }

    @Test
    public void secondTest() throws InterruptedException {
        var executor = new ExecutorImpl(new TestPipelineOptions());
        var server = new StatisticsHandling.NIOServer(
                new CoordinatorImpl(
                        new CostEstimatorImpl(),
                        executor
                ),
                1112
        );

        var client = new StatisticsHandling.MessageSender(
                "my message",
                new InetSocketAddress("localhost", 1112)
        );

        var serverThread = new Thread(server);
        serverThread.start();

        Thread.sleep(100);
        client.send();
        Thread.sleep(100);
        assertEquals("my message", server.result);

    }
}
