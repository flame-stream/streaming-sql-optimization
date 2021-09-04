package com.flamestream.optimizer.sql.agents.impl;

import com.flamestream.optimizer.sql.agents.Services;
import com.flamestream.optimizer.sql.agents.StatsServiceGrpc;
import com.google.protobuf.Empty;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StatisticsHandling {
    private static final Metadata.Key<String> TAG = Metadata.Key.of("tag", Metadata.ASCII_STRING_MARSHALLER);

    public static class StatsOutput extends PTransform<PCollection<Long>, PDone> {
        public StatsOutput(String tag) {
            this.tag = tag;
        }

        private final String tag;

        @Override
        public PDone expand(PCollection<Long> input) {
            input.apply(ParDo.of(new DoFn<Long, Void>() {
                @ProcessElement
                public void processElement(ProcessContext c, BoundedWindow window) {
                    try (final var statsSender = new StatsSender(new InetSocketAddress("localhost", 1111), tag)) {
                        statsSender.send(
                                c.timestamp().getMillis(),
                                Collections.singletonMap("", c.element().doubleValue())
                        );
                    }
                }
            }));
            return PDone.in(input.getPipeline());
        }
    }

    public static class NIOServer implements Runnable {
        private final int port;

        public NIOServer(CoordinatorImpl coordinator, int port) {
            this.coordinator = coordinator;
            this.port = port;
        }

        public final CoordinatorImpl coordinator;
        public String result;

        @Override
        @SuppressWarnings("unused")
        public void run() {
            final var tag = Context.key("user-agent");
            final var server = ServerBuilder.forPort(port).intercept(new ServerInterceptor() {
                @Override
                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next
                ) {
                    return Contexts.interceptCall(
                            Context.current().withValue(tag, headers.get(TAG)),
                            call, headers, next
                    );
                }
            }).addService(new StatsServiceGrpc.StatsServiceImplBase() {
                @Override
                public StreamObserver<Services.Stats> push(StreamObserver<Empty> responseObserver) {
                    result = (String) tag.get();
                    return new StreamObserver<>() {
                        @Override
                        public void onNext(Services.Stats value) {
                            log("Message received: " + value);
                        }

                        @Override
                        public void onError(Throwable t) {
                            log(t.toString());
                        }

                        @Override
                        public void onCompleted() {
                            responseObserver.onCompleted();
                        }
                    };
                }
            }).build();
            try (final Closeable ignored = server::shutdown) {
                server.start();
                while (true) {
                    try {
                        server.awaitTermination();
                        break;
                    } catch (InterruptedException e) {
                        server.shutdownNow();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void log(String str) {
            System.out.println(str);
        }
    }

    public static class StatsSender implements AutoCloseable {
        private final ManagedChannel managedChannel;
        private final StreamObserver<Services.Stats> stats;

        public StatsSender(InetSocketAddress address, String userAgent) {
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
                                    headers.put(TAG, userAgent);
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
