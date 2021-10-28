package com.flamestream.optimizer.sql.agents.impl;

import com.flamestream.optimizer.sql.agents.Executor;
import com.flamestream.optimizer.sql.agents.Services;
import com.flamestream.optimizer.sql.agents.WorkerServiceGrpc;
import com.flamestream.optimizer.sql.agents.source.SourceWrapper;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ExecutorImpl implements Executor, Serializable {
    private Pipeline currentPipeline = null;
    private SourceCommunicator sourceCommunicator = null;
    // some kind of watermark
    private long timestamp = -1;
    private final PipelineOptions options;
    private CountDownLatch latch = null;

    private final List<SourceCommunicator> sources;

    // TODO i have zero idea
    private final String tag = "user-agent";

    public ExecutorImpl(final PipelineOptions options) {
        this.options = options;
        this.sources = new ArrayList<>();
    }

    @Override
    public void submitSource(String sourceHostAndPort) {
        final String host = sourceHostAndPort.substring(0, sourceHostAndPort.lastIndexOf(':'));
        final int port = Integer.parseInt(sourceHostAndPort.substring(sourceHostAndPort.lastIndexOf(':') + 1));
        sources.add(new SourceCommunicator(new InetSocketAddress(host, port), tag));
        if (latch != null) {
            latch.countDown();
        }
    }

    @Override
    public void startOrUpdate(Pipeline pipeline, Consumer<ChangingStatus> statusConsumer) throws InterruptedException {
        final PipelineRunner<@NonNull PipelineResult> runner = FlinkRunner.fromOptions(options);
        runner.run(pipeline);

        if (currentPipeline != null) {
            for (SourceCommunicator source : sources) {
                source.pause();
            }
            latch = new CountDownLatch(sources.size());
            latch.await();
            /*for (SourceCommunicator source : sources) {
                source.resumeAtTimestamp();
            }*/
        }


        currentPipeline = pipeline;
        // TODO random choice of available port?
        /*sourceCommunicator = new SourceCommunicator(new InetSocketAddress("localhost", 9000), tag);
        sourceCommunicator.resumeAtTimestamp(timestamp);*/
    }

    @Nullable
    @Override
    public Pipeline current() {
        return currentPipeline;
    }

    public static class SourceCommunicator implements AutoCloseable {
        private final ManagedChannel managedChannel;
        private final WorkerServiceGrpc.WorkerServiceStub stub;
        private long timestamp;

        public SourceCommunicator(InetSocketAddress address, String userAgent) {
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
                                    headers.put(SourceWrapper.TAG, userAgent);
                                    super.start(responseListener, headers);
                                }
                            };
                        }
                    }).build();
            stub = WorkerServiceGrpc.newStub(managedChannel);
        }

        public void pause() {
            stub.pause(Services.Empty.newBuilder().build(), new StreamObserver<>() {
                @Override
                public void onNext(Services.Timestamp value) {
                    timestamp = value.getValue();
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {

                }
            });
        }

        public void resumeAtTimestamp(final long timestamp) {
            stub.resumeTo(Services.Timestamp.newBuilder().setValue(timestamp).build(), new StreamObserver<>() {
                @Override
                public void onNext(Services.Empty value) {

                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {

                }
            });
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
