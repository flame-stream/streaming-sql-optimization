package com.flamestream.optimizer.sql.agents.impl;

import com.flamestream.optimizer.sql.agents.Services;
import com.flamestream.optimizer.sql.agents.StatsServiceGrpc;
import com.flamestream.optimizer.sql.agents.util.NetworkUtil;
import com.google.protobuf.Empty;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class StatisticsHandling {
    private static final Metadata.Key<String> TARGET_KEY = Metadata.Key.of("target", Metadata.ASCII_STRING_MARSHALLER);

    private static final Logger LOG = LoggerFactory.getLogger("optimizer.statistics");

    public static class LocalCardinalityDoFn extends DoFn<KV<Long, Long>, KV<String, Double>> {
        private final String column;
        @StateId("void")
        private final StateSpec<ValueState<Void>> ignored = StateSpecs.value();
        private BoundedWindow boundedWindow = null;
        private int i = 0;

        public LocalCardinalityDoFn(String column) {
            this.column = column;
        }

        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) {
        }

        @OnWindowExpiration
        public void onWindowExpiration(BoundedWindow boundedWindow) {
            this.boundedWindow = boundedWindow;
            ++i;
        }

        @FinishBundle
        public void finishBundle(FinishBundleContext finishBundleContext) {
            if (finishBundleContext == null || boundedWindow == null) {
                return;
            }
            LOG.info("stats for pipeline " + finishBundleContext.getPipelineOptions().getJobName() + " column " + column + " value " + i);
            finishBundleContext.output(KV.of(column, (double) i), boundedWindow.maxTimestamp(), boundedWindow);
            i = 0;
            boundedWindow = null;
        }
    }

    public static class StatsDoFn extends DoFn<KV<String, Double>, Void> {
        private final InetSocketAddress executorAddress;
        private final int cardinalitiesNumber;
        private String userAgent;

        public StatsDoFn(InetSocketAddress executorAddress, int cardinalitiesNumber) {
            this.executorAddress = executorAddress;
            this.cardinalitiesNumber = cardinalitiesNumber;
        }

        private StatsSender statsSender;

        private final TreeMap<Instant, Map<String, Double>> pendingCardinality = new TreeMap<>();

        @Setup
        public void setup() throws IOException {
            final int statsPort = ThreadLocalRandom.current().nextInt(9000, 10000);
            LOG.info("new stats port " + statsPort);
            userAgent = NetworkUtil.getLocalAddressHost(executorAddress) + ":" + statsPort;
            statsSender = new StatsSender(executorAddress, userAgent);
        }

        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) {
            if (
                    pendingCardinality.computeIfAbsent(window.maxTimestamp(), __ -> new HashMap<>())
                            .putIfAbsent(c.element().getKey(), c.element().getValue()) != null
            ) {
                throw new IllegalStateException();
            }
        }

        @FinishBundle
        public void finishBundle() {
            final var timeCardinalityIterator = pendingCardinality.entrySet().iterator();
            while (timeCardinalityIterator.hasNext()) {
                final var timeCardinality = timeCardinalityIterator.next();
                if (timeCardinality.getValue().size() == cardinalitiesNumber) {
                    final var millis = timeCardinality.getKey().getMillis();
                    statsSender.send(millis, timeCardinality.getValue());
                    timeCardinalityIterator.remove();
                }
            }
        }

        @Teardown
        public void teardown() {
            statsSender.close();
        }

        private InetAddress getLocalAddress() throws SocketException {
            try (final var datagramSocket = new DatagramSocket()) {
                datagramSocket.connect(executorAddress);
                return datagramSocket.getLocalAddress();
            }
        }
    }

    public static class NIOServer implements AutoCloseable {
        private final Server server;

        public NIOServer(
                int port, Function<String, StreamObserver<Services.Stats>> targetStatsObserver
        ) throws IOException {
            final var targetKey = Context.key("target");
            this.server = ServerBuilder.forPort(port).intercept(new ServerInterceptor() {
                @Override
                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next
                ) {
                    return Contexts.interceptCall(
                            Context.current().withValue(targetKey, headers.get(TARGET_KEY)),
                            call, headers, next
                    );
                }
            }).addService(new StatsServiceGrpc.StatsServiceImplBase() {
                @Override
                public StreamObserver<Services.Stats> push(StreamObserver<Empty> responseObserver) {
                    final var statsStreamObserver = targetStatsObserver.apply((String) targetKey.get());
                    return new StreamObserver<>() {
                        @Override
                        public void onNext(Services.Stats value) {
                            statsStreamObserver.onNext(value);
                        }

                        @Override
                        public void onError(Throwable t) {
                            statsStreamObserver.onError(t);
                        }

                        @Override
                        public void onCompleted() {
                            statsStreamObserver.onCompleted();
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                        }
                    };
                }
            }).build();
            LOG.info("started statistics server");
            server.start();
        }

        public String result;

        @Override
        public void close() throws Exception {
            LOG.info("closing statistics server");
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

    public static class StatsSender implements AutoCloseable {
        private final ManagedChannel managedChannel;
        private final StreamObserver<Services.Stats> stats;

        public StatsSender(InetSocketAddress address, String target
                //, List<String> sourceAddresses
        ) {
            LOG.info("called stats sender constructor");
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
                                    headers.put(TARGET_KEY, target);
                                    super.start(responseListener, headers);
                                }
                            };
                        }
                    }).build();
            stats = StatsServiceGrpc.newStub(managedChannel).push(new StreamObserver<>() {
                @Override
                public void onNext(Empty value) {
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                }

                @Override
                public void onCompleted() {
                }
            });
        }

        public void send(long timestamp, Map<String, Double> cardinalities) {
            stats.onNext(Services.Stats.newBuilder().setTimestamp(timestamp).putAllCardinality(cardinalities).build());
        }

        @Override
        public void close() {
            stats.onCompleted();
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
