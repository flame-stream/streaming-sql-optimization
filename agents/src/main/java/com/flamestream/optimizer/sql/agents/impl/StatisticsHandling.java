package com.flamestream.optimizer.sql.agents.impl;

import com.flamestream.optimizer.sql.agents.Services;
import com.flamestream.optimizer.sql.agents.StatsServiceGrpc;
import com.flamestream.optimizer.sql.agents.source.SourceWrapper;
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

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

public class StatisticsHandling {
    private static final Metadata.Key<String> TARGET_KEY = Metadata.Key.of("target", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> WORKER_KEY = Metadata.Key.of("worker", Metadata.ASCII_STRING_MARSHALLER);
    public static final int STATS_PORT = 9004;
    public static int workerPortNumber = 10000;

    public static class LocalCardinalityDoFn extends DoFn<KV<Long, Long>, KV<String, Double>> {
        private final String column;
        @StateId("void")
        private final StateSpec<ValueState<Void>> ignored = StateSpecs.value();
        private BoundedWindow boundedWindow;
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
            finishBundleContext.output(KV.of(column, (double) i), boundedWindow.maxTimestamp(), boundedWindow);
            i = 0;
        }
    }

    public static class StatsDoFn extends DoFn<KV<String, Double>, Void> {
        private final InetSocketAddress executorAddress;
        private final int cardinalitiesNumber;
        private String userAgent;
        private final List<String> sourceAddresses;

        public StatsDoFn(InetSocketAddress executorAddress, int cardinalitiesNumber, List<String> sourceAddresses) {
            this.executorAddress = executorAddress;
            this.cardinalitiesNumber = cardinalitiesNumber;
            this.sourceAddresses = sourceAddresses;
        }

        private StatsSender statsSender;

        private final TreeMap<Instant, Map<String, Double>> pendingCardinality = new TreeMap<>();

        @Setup
        public void setup() throws IOException {
            userAgent = getLocalAddress().getHostAddress() + ":" + STATS_PORT;
            System.out.println("user agent" + userAgent);
            statsSender = new StatsSender(executorAddress, userAgent, sourceAddresses);
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
                int port, BiFunction<String, String, StreamObserver<Services.Stats>> targetStatsObserver
        ) throws IOException {
            final var targetKey = Context.key("target");
            final var workerKey = Context.key("worker");
            this.server = ServerBuilder.forPort(port).intercept(new ServerInterceptor() {
                @Override
                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next
                ) {
                    System.out.println("worker key" + headers.get(WORKER_KEY));
                    return Contexts.interceptCall(
                            Context.current()
                                    .withValue(targetKey, headers.get(TARGET_KEY))
                                    .withValue(workerKey, headers.get(WORKER_KEY)),
                            call, headers, next
                    );
                }
            }).addService(new StatsServiceGrpc.StatsServiceImplBase() {
                @Override
                public StreamObserver<Services.Stats> push(StreamObserver<Empty> responseObserver) {
                    final var statsStreamObserver = targetStatsObserver.apply((String) targetKey.get(), (String) workerKey.get());
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
            server.start();
        }

        public String result;

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

    public static class StatsSender implements AutoCloseable {
        private final ManagedChannel managedChannel;
        private final StreamObserver<Services.Stats> stats;

        public StatsSender(InetSocketAddress address, String target, List<String> sourceAddresses) {
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
                                    headers.put(WORKER_KEY, String.join(" ", sourceAddresses));
                                    System.out.println("on client: " + String.join(" ", sourceAddresses));
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
