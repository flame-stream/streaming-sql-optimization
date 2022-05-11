package com.flamestream.optimizer.sql.agents.impl;

import com.flamestream.optimizer.sql.agents.AddressServiceGrpc;
import com.flamestream.optimizer.sql.agents.Executor;
import com.flamestream.optimizer.sql.agents.Services;
import com.flamestream.optimizer.sql.agents.WorkerServiceGrpc;
import com.google.protobuf.Empty;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.beam.runners.flink.FlinkDetachedRunnerResultWithJobClient;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.flink.core.execution.JobClient;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ExecutorImpl implements Executor, Serializable, AutoCloseable {
    public static final Logger LOG = LoggerFactory.getLogger("optimizer.executor");

    private boolean running = false;
//    private Pipeline currentPipeline = null;
//    private final PipelineOptions options;
    private final String optionsArguments;

    private CountDownLatch newGraphLatch = null;

    private final List<SourceCommunicator> currentSources = new ArrayList<>();
    private final List<SourceCommunicator> newSources = new ArrayList<>();

    private final AddressesServer addressesServer;

    private JobClient client = null;

    // TODO will be replaced with a list or smth
    private int jobCounter = 0;

    public ExecutorImpl(final String optionsArguments) {
        this.optionsArguments = optionsArguments;
        try {
            addressesServer = new AddressesServer(30303);
        } catch (IOException e) {
            LOG.error("error when initializing address server in executor", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        try (addressesServer) {
        }
    }

    @Override
    public void submitSource(String sourceHostAndPort) {
        final String host = sourceHostAndPort.substring(0, sourceHostAndPort.lastIndexOf(':'));
        final int port = Integer.parseInt(sourceHostAndPort.substring(sourceHostAndPort.lastIndexOf(':') + 1));
        LOG.info("host and port submitted " + sourceHostAndPort);
        // TODO blatant hack, replace with some vaguely coherent logic later, like labeling each source with a pipeline number or something
        if (newSources.stream().noneMatch(it -> it.address.getHostName().equals(host) && it.address.getPort() == port)) {
            LOG.info("adding source on executor: " + sourceHostAndPort);
            final SourceCommunicator source = new SourceCommunicator(new InetSocketAddress(host, port));
            newSources.add(source);
        }
        if (newGraphLatch != null) {
            LOG.info("counting down the latch for " + sourceHostAndPort);
            newGraphLatch.countDown();
        }
    }

    @Override
    public void startOrUpdate(Pipeline pipeline, Consumer<ChangingStatus> statusConsumer) throws InterruptedException {
        LOG.info("start or update pipeline");

//        System.out.println(PipelineDotRenderer.toDotString(pipeline));
        // turns out job name is set from the options that are set in the runner
        // so now we have two runners for two different jobs with, most importantly, two different names
        // btw options cannot be copied, only recreated from args apparently,
        // hence passing the args string here, which is terrible and bizarre
        final FlinkPipelineOptions oldOptions = PipelineOptionsFactory.fromArgs(optionsArguments.split(" ")).withValidation().as(FlinkPipelineOptions.class);
        oldOptions.setJobName("old_graph");
        final PipelineRunner<@NonNull PipelineResult> oldRunner = FlinkRunner.fromOptions(oldOptions);
        final PipelineOptions newOptions = PipelineOptionsFactory.fromArgs(optionsArguments.split(" ")).withValidation().as(FlinkPipelineOptions.class);
        newOptions.setJobName("new_graph" + jobCounter);
        final PipelineRunner<@NonNull PipelineResult> newRunner = FlinkRunner.fromOptions(newOptions);


//        LOG.info("current pipeline is not null " + (currentPipeline != null));
        if (running) {
            if (jobCounter == 0) {
                // old graph is currently in new sources -- transfer it to current sources
                currentSources.clear();
                currentSources.addAll(newSources);
                newSources.clear();
            }

            LOG.info("current sources " + currentSources.stream().map(it -> it.address.toString()).collect(Collectors.joining(" ")));

            CountDownLatch pauseLatch = new CountDownLatch(currentSources.size());
            newGraphLatch = new CountDownLatch(currentSources.size());

            jobCounter++;

            PipelineResult result = null;
            try {
                result = newRunner.run(pipeline);
            } catch (RuntimeException e) {
                LOG.error("error", e);
                System.err.println(e.getMessage());
            }

            LOG.info("about to wait on latch");
            newGraphLatch.await();

            final List<Long> watermarks = new ArrayList<>();
            for (SourceCommunicator source : currentSources) {
                LOG.info("calling pause on source " + source.address);
                source.pause(watermarks, pauseLatch);
            }

            LOG.info("wait on pause latch");
            pauseLatch.await();
            LOG.info("pause latch is ok actually");
            final long maxWatermark = watermarks.isEmpty() ? -1 : Collections.max(watermarks);
            if (maxWatermark == -1) {
                LOG.info("no watermarks");
            } else {
                for (SourceCommunicator source : currentSources) {
                    LOG.info("calling halt on executor " + source.address);
                    source.halt(maxWatermark);
                }

//                currentPipeline = pipeline;
                currentSources.clear();
                currentSources.addAll(newSources);
                newSources.clear();
                for (SourceCommunicator source : currentSources) {
                    source.resumeTo(maxWatermark);
                }
            }


            if (client != null) {
                client.cancel();
                if (result instanceof FlinkDetachedRunnerResultWithJobClient) {
                    client = ((FlinkDetachedRunnerResultWithJobClient) result).getJobClient();
                }
            }
        }
        else {
            running = true;
            PipelineResult result = oldRunner.run(pipeline);
            if (result instanceof FlinkDetachedRunnerResultWithJobClient) {
                client = ((FlinkDetachedRunnerResultWithJobClient) result).getJobClient();
            }
        }
    }

    @Nullable
    @Override
    public Pipeline current() {
        return null;
        //return currentPipeline;
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

        public void pause(final List<Long> watermarks, final CountDownLatch pauseLatch) {
            Context newContext = Context.current().fork();
            Context origContext = newContext.attach();
            try {
                stub.pause(Services.Empty.newBuilder().getDefaultInstanceForType(), new StreamObserver<>() {
                    @Override
                    public void onNext(Services.Timestamp value) {
                        LOG.info("next " + value.getValue());
                        watermark = Instant.ofEpochMilli(value.getValue());
                        watermarks.add(watermark.getMillis());
                        pauseLatch.countDown();
                    }

                    @Override
                    public void onError(Throwable t) {
                        LOG.error("an error reported by executor in pause", t);
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
            } finally {
                newContext.detach(origContext);
            }
        }

        public void halt(long watermark) {
            Context newContext = Context.current().fork();
            Context origContext = newContext.attach();
            try {
                stub.halt(Services.Timestamp.newBuilder().setValue(watermark).build(), new StreamObserver<>() {
                    @Override
                    public void onNext(Services.Empty value) {
                        LOG.info("called halt at " + watermark + " on executor client");
                    }

                    @Override
                    public void onError(Throwable t) {
                        LOG.error("an error reported by executor client in resume to", t);
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
                stub.resumeTo(Services.Timestamp.newBuilder().setValue(watermark).build(), new StreamObserver<>() {
                    @Override
                    public void onNext(Services.Empty value) {
                        LOG.info("called resume to " + watermark + " on executor client");
                    }

                    @Override
                    public void onError(Throwable t) {
                        LOG.error("an error reported by executor client in resume to", t);
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

    public class AddressesServer implements AutoCloseable {
        private final Server server;

        public AddressesServer(int port) throws IOException {
            this.server = ServerBuilder.forPort(port).intercept(new ServerInterceptor() {
                @Override
                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next
                ) {
                    return Contexts.interceptCall(Context.current(), call, headers, next);
                }
            }).addService(new AddressServiceGrpc.AddressServiceImplBase() {
                @Override
                public void sendAddress(Services.Address request, StreamObserver<Empty> responseObserver) {
                    submitSource(request.getAddress());
                }
            }).build();
            server.start();
        }

        @Override
        public void close() throws Exception {
            server.shutdown();
            while (true) {
                try {
                    server.awaitTermination();
                    break;
                } catch (InterruptedException e) {
                    server.shutdownNow();
                }
            }
        }
    }
}
