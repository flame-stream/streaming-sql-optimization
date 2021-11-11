package com.flamestream.optimizer.sql.agents.impl;

import com.flamestream.optimizer.sql.agents.Executor;
import com.flamestream.optimizer.sql.agents.Services;
import com.flamestream.optimizer.sql.agents.WorkerServiceGrpc;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ExecutorImpl implements Executor, Serializable {
    // this vile thing is used to call tryNewGraph only once and will remain here
    // until we implement some logic which will allow us to decide when to create a new graph
    public static int HACKY_VARIABLE = 0;

    public static final Logger LOG = LoggerFactory.getLogger("optimizer.executor");

    private Pipeline currentPipeline = null;
    private final PipelineOptions options;
    private CountDownLatch latch = null;

    private final List<SourceCommunicator> currentSources;

    // TODO i have zero idea
    private final String tag = "user-agent";

    public ExecutorImpl(final PipelineOptions options) {
        this.options = options;
        currentSources = new ArrayList<>();
    }

    @Override
    public void submitSource(String sourceHostAndPort) {
        final String host = sourceHostAndPort.substring(0, sourceHostAndPort.lastIndexOf(':'));
        final int port = Integer.parseInt(sourceHostAndPort.substring(sourceHostAndPort.lastIndexOf(':') + 1));
        // TODO blatant hack, replace with some vaguely coherent logic later, like labeling each source with a pipeline number or something
        if (currentSources.stream().noneMatch(it -> it.address.getHostName().equals(host) && it.address.getPort() == port)) {
            LOG.info("adding source on executor: " + sourceHostAndPort);
            final SourceCommunicator source = new SourceCommunicator(new InetSocketAddress(host, port));
            currentSources.add(source);
        }
        if (latch != null) {
            LOG.info("counting down the latch");
            latch.countDown();
        }
    }

    @Override
    public void startOrUpdate(Pipeline pipeline, Consumer<ChangingStatus> statusConsumer) throws InterruptedException {
        LOG.info("start or update pipeline");

        HACKY_VARIABLE++;
        final PipelineRunner<@NonNull PipelineResult> runner = FlinkRunner.fromOptions(options);

        LOG.info("current pipeline is not null " + (currentPipeline != null));
        if (currentPipeline != null) {
            // TODO replace instants with longs and all that, unnecessary complications
            final List<Long> watermarks = new ArrayList<>();
            for (SourceCommunicator source : currentSources) {
                LOG.info("calling pause on source " + source.address);
                source.pause(watermarks);
            }
            latch = new CountDownLatch(currentSources.size());

            // TODO maybe not
            currentSources.clear();

            final PipelineResult res = runner.run(pipeline);
            LOG.info("about to wait on latch");
            latch.await();
            currentPipeline = pipeline;

            final long maxWatermark = Collections.max(watermarks);
            for (SourceCommunicator source : currentSources) {
                source.resumeTo(maxWatermark);
            }

            // TODO should we?
            res.waitUntilFinish();
        }
        else {
            currentPipeline = pipeline;
            runner.run(pipeline);
        }
    }

    @Nullable
    @Override
    public Pipeline current() {
        return currentPipeline;
    }

    public static class SourceCommunicator implements AutoCloseable {
        private final ManagedChannel managedChannel;
        private final WorkerServiceGrpc.WorkerServiceStub stub;
        private final InetSocketAddress address;
        private Instant watermark;

        public SourceCommunicator(InetSocketAddress address) {
            LOG.info("called source communicator constructor");
            this.address = address;
            managedChannel = ManagedChannelBuilder.forAddress(address.getHostName(), address.getPort())
                    .usePlaintext().intercept(new ClientInterceptor() {
                        @Override
                        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next
                        ) {
                            return new ForwardingClientCall.SimpleForwardingClientCall<>(
                                    next.newCall(method, callOptions)
                            ) {
                                @Override
                                public void start(Listener<RespT> responseListener, Metadata headers) {
                                    super.start(responseListener, headers);
                                }
                            };
                        }
                    }).build();
            stub = WorkerServiceGrpc.newStub(managedChannel);
        }

        public void pause(final List<Long> watermarks) {
            Context newContext = Context.current().fork();
            Context origContext = newContext.attach();
            try {
                // Call async RPC here
                stub.pause(Services.Empty.newBuilder().getDefaultInstanceForType(), new StreamObserver<>() {
                    @Override
                    public void onNext(Services.Timestamp value) {
                        LOG.info("next " + value.getValue());
                        watermark = Instant.ofEpochMilli(value.getValue());
                        watermarks.add(watermark.getMillis());
                    }

                    @Override
                    public void onError(Throwable t) {
                        LOG.error(t.getMessage());
                        t.printStackTrace();
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
            } finally {
                newContext.detach(origContext);
            }
        }

        public void resumeTo(long watermark) {
            Context newContext = Context.current().fork();
            Context origContext = newContext.attach();
            try {
                // Call async RPC here
                LOG.info("calling async rpc. why does this lead to OOM?");
                stub.resumeTo(Services.Timestamp.newBuilder().setValue(watermark).build(), new StreamObserver<>() {
                    @Override
                    public void onNext(Services.Empty value) {
                        LOG.info("called resume to " + watermark + " on executor client");
                    }

                    @Override
                    public void onError(Throwable t) {
                        LOG.info("there was an error which is now being reported by executor client");
                        t.printStackTrace();
                    }

                    @Override
                    public void onCompleted() {

                    }
                });
            } finally {
                newContext.detach(origContext);
            }
        }

        @Override
        public void close() {
            managedChannel.shutdown();
            while (true) {
                try {
                    managedChannel.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
                    break;
                } catch (InterruptedException e) {
                    managedChannel.shutdownNow();
                }
            }
        }
    }
}
