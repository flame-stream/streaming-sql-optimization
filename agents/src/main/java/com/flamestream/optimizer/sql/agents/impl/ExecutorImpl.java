package com.flamestream.optimizer.sql.agents.impl;

import com.flamestream.optimizer.sql.agents.Executor;
import com.flamestream.optimizer.sql.agents.Services;
import com.flamestream.optimizer.sql.agents.StatsServiceGrpc;
import com.flamestream.optimizer.sql.agents.WorkerServiceGrpc;
import com.flamestream.optimizer.sql.agents.source.SourceWrapper;
import com.google.protobuf.Empty;
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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class ExecutorImpl implements Executor, Serializable {
    private Pipeline currentPipeline = null;
    private SourceCommunicator sourceCommunicator = null;
    // some kind of watermark
    private long timestamp = -1;
    private final PipelineOptions options;

    // TODO i have zero idea
    private final String tag = "user-agent";

    public ExecutorImpl(final PipelineOptions options) {
        this.options = options;
    }

    @Override
    public void startOrUpdate(Pipeline pipeline, Consumer<ChangingStatus> statusConsumer) {
        if (currentPipeline != null) {
            sourceCommunicator.pause();
            // TODO wait until it has been completed, so we need to get an acknowledgement from the source somehow
        }


        currentPipeline = pipeline;
        // TODO random choice of available port?
        sourceCommunicator = new SourceCommunicator(new InetSocketAddress("localhost", 9000), tag);
        sourceCommunicator.resumeAtTimestamp(timestamp);

        final PipelineRunner<@NonNull PipelineResult> runner = FlinkRunner.fromOptions(options);
        runner.run(pipeline);
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
