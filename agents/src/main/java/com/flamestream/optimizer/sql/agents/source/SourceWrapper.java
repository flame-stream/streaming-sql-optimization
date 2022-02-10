package com.flamestream.optimizer.sql.agents.source;

import com.flamestream.optimizer.sql.agents.AddressServiceGrpc;
import com.flamestream.optimizer.sql.agents.CheckpointServiceGrpc;
import com.flamestream.optimizer.sql.agents.Services;
import com.flamestream.optimizer.sql.agents.WorkerServiceGrpc;
import com.flamestream.optimizer.sql.agents.util.NetworkUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

// TODO fix nullability annotations, remove all other annotations
public class SourceWrapper<T, U extends UnboundedSource.CheckpointMark> extends UnboundedSource<T, U> {
    public static final Metadata.Key<String> CHECKPOINT_TAG = Metadata.Key.of("checkpoint_tag", Metadata.ASCII_STRING_MARSHALLER);

    public static final Logger LOG = LoggerFactory.getLogger("optimizer.source");

    private final int portNumber;

    private final UnboundedSource<T, U> source;

    private final byte[] checkpointMarkSerialized;

    private final boolean holdUntilResume;

    public SourceWrapper(final UnboundedSource<T, U> source) {
        this(source, false, null);
    }

    public SourceWrapper(final UnboundedSource<T, U> source, boolean holdUntilResume) {
        this(source, holdUntilResume, null);
    }

    public SourceWrapper(final UnboundedSource<T, U> source, boolean holdUntilResume, final byte[] mark) {
        this.source = source;
        this.holdUntilResume = holdUntilResume;
        this.portNumber = ThreadLocalRandom.current().nextInt(9000, 65000);
        this.checkpointMarkSerialized = mark;
        LOG.info("chose port number " + this.portNumber);
        LOG.info("hold until resume " + holdUntilResume);
    }

    public int getPortNumber() {
        return portNumber;
    }

    @Override
    public List<SourceWrapper<T, U>> split(int desiredNumSplits, PipelineOptions options) throws Exception {
        LOG.info("split");
        return List.of(this);
    }

    @Override
    public @NonNull UnboundedReader<T> createReader(@NonNull PipelineOptions options,
                                                    @Nullable U checkpointMark) throws IOException {
        U checkpointDeserialized = null;
        if (checkpointMarkSerialized != null) {
            ByteArrayInputStream bais = new ByteArrayInputStream(checkpointMarkSerialized);
            checkpointDeserialized = getCheckpointMarkCoder().decode(bais);
        }
        final U checkpoint = checkpointMark == null && checkpointDeserialized != null ? checkpointDeserialized : checkpointMark;
        LOG.info("created reader at checkpoint " + checkpoint);
        return new ReaderWrapper(source.createReader(options, checkpoint));
    }

    @Override
    public @NonNull Coder<U> getCheckpointMarkCoder() {
        return source.getCheckpointMarkCoder();
    }

    @Override
    public @NonNull Coder<T> getOutputCoder() {
        return source.getOutputCoder();
    }

    private class ReaderWrapper extends UnboundedReader<T> {
        private final UnboundedReader<T> reader;
        private final WorkerServer workerServer;
        private final CheckpointsSender checkpointsSender;

        private boolean startedEmitting = false;

        private ReaderWrapper(final UnboundedReader<T> reader) throws IOException {
            this.reader = reader;
            LOG.info("started worker server on source on port " + portNumber);
            workerServer = new WorkerServer(portNumber);
            checkpointsSender = new CheckpointsSender(new InetSocketAddress(20202), getPortNumber());
            AddressSender addressSender = new AddressSender(new InetSocketAddress(30303));
            String host = NetworkUtil.getLocalAddressHost(new InetSocketAddress(30303));
            addressSender.sendAddress(host + ":" + getPortNumber());
        }

        @Override
        public boolean start() throws IOException {
            return advance();
        }

        @Override
        public boolean advance() throws IOException {
            final boolean res = reader.advance();

//            LOG.info(workerServer.resultRef.get() + " " + portNumber);
            if (!"Resume".equals(workerServer.resultRef.get()) && holdUntilResume) {
                return false;
            }

            if (workerServer.resultRef.get() != null) {
                Instant current = getCurrentTimestamp();
                if (workerServer.resultRef.get().equals("Halt")) {
                    if (current.getMillis() > workerServer.watermark.getMillis()) {
                        return false;
                    }
                }
                if (workerServer.resultRef.get().equals("Resume")) {
                    while (current.getMillis() <= workerServer.watermark.getMillis()) {
                        reader.getCurrent();
                        reader.advance();
                        current = getCurrentTimestamp();
                    }
                }
            }

            if (!startedEmitting) {
                startedEmitting = true;
                LOG.info("started emitting");
            }

            return res;
        }

        @Override
        public T getCurrent() throws NoSuchElementException {
            final T res = reader.getCurrent();

            if (workerServer.resultRef.get() == null && holdUntilResume) {
                LOG.info("holding until resume");
                return null;
            }

            if (workerServer.resultRef.get() != null) {
                final Instant current = getCurrentTimestamp();
                if (workerServer.resultRef.get().equals("Halt")) {
                    if (current.getMillis() > workerServer.watermark.getMillis()) {
                        LOG.info("this halt has served its purpose in getCurrent");
                        return null;
                    }
                }
                if (workerServer.resultRef.get().equals("Resume")) {
                    // technically this should never happen
                    if (current.getMillis() <= workerServer.watermark.getMillis()) {
                        return null;
                    }
                }
            }

            /*String text = "";
            if (res instanceof Event) {
                if (((Event) res).newPerson != null) {
                    Person p = ((Event) res).newPerson;
                    text = p.id + " " + p.dateTime;
                }
                if (((Event) res).newAuction != null) {
                    Auction p = ((Event) res).newAuction;
                    text = p.id + " " + p.dateTime;
                }
                if (((Event) res).bid != null) {
                    Bid p = ((Event) res).bid;
                    text = p.bidder + " " + p.auction + " " + p.dateTime;
                }
            }
            LOG.info("element " + text + " on port " + getPortNumber());*/

            return res;
        }

        @Override
        public @NonNull Instant getCurrentTimestamp() throws NoSuchElementException {
            return reader.getCurrentTimestamp();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public @NonNull Instant getWatermark() {
            Instant watermark = reader.getWatermark();
            // this cast should succeed
            checkpointsSender.send((U)getCheckpointMark());
//            LOG.info("watermark " + watermark.getMillis() + " on port " + portNumber);
            return watermark;
        }

        @Override
        public @NonNull CheckpointMark getCheckpointMark() {
            return reader.getCheckpointMark();
        }

        @Override
        public @NonNull UnboundedSource<T, ?> getCurrentSource() {
            return reader.getCurrentSource();
        }

        private class WorkerServer implements AutoCloseable {
            private final Server server;
            private Instant watermark;

            public WorkerServer(int port) throws IOException {
                this.server = ServerBuilder.forPort(port).intercept(new ServerInterceptor() {
                    @Override
                    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next
                    ) {
                        return Contexts.interceptCall(
                                Context.current(),
                                call, headers, next
                        );
                    }
                }).addService(new WorkerServiceGrpc.WorkerServiceImplBase() {
                    @Override
                    public void pause(Services.Empty request, StreamObserver<Services.Timestamp> responseObserver) {
                        LOG.info("pause called on server on source");
                        resultRef.set("Pause");
//                        result = "Pause"; // Looks Bad
                        responseObserver.onNext(Services.Timestamp.newBuilder().setValue(getWatermark().getMillis()).build());
                    }

                    @Override
                    public void halt(Services.Timestamp request, StreamObserver<Services.Empty> responseObserver) {
                        LOG.info("halt called on server on source");
                        resultRef.compareAndSet("Pause", "Halt");
                        watermark = Instant.ofEpochMilli(request.getValue());
                        responseObserver.onNext(Services.Empty.newBuilder().getDefaultInstanceForType());
                    }

                    @Override
                    public void resumeTo(Services.Timestamp request, StreamObserver<Services.Empty> responseObserver) {
                        LOG.info("resume called on server on source");
                        resultRef.set("Resume");
//                        result = "Resume";
                        watermark = Instant.ofEpochMilli(request.getValue());
                        responseObserver.onNext(Services.Empty.newBuilder().getDefaultInstanceForType());
                    }
                }).build();
                LOG.info("started worker server");
                server.start();
            }

            public String result;
            public AtomicReference<String> resultRef = new AtomicReference<>();

            @Override
            public void close() throws Exception {
                LOG.info("closing worker server");
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

        private class CheckpointsSender implements AutoCloseable, Serializable {
            private final ManagedChannel managedChannel;
            private final StreamObserver<Services.Checkpoint> checkpoints;

            public CheckpointsSender(final InetSocketAddress address, int portNumber) {
                LOG.info("called checkpoints sender constructor");
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
                                        String host = NetworkUtil.getLocalAddressHost(address);
                                        headers.put(CHECKPOINT_TAG, host + ":" + portNumber);
                                        super.start(responseListener, headers);
                                    }
                                };
                            }
                        }).build();
                checkpoints = CheckpointServiceGrpc.newStub(managedChannel).push(new StreamObserver<>() {
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

            public void send(U checkpoint) {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try {
                    getCheckpointMarkCoder().encode(checkpoint, baos);
                } catch (IOException e) {
                    LOG.info("error while encoding the checkpoint", e);
                }
                checkpoints.onNext(Services.Checkpoint.newBuilder().setCheckpoint(ByteString.copyFrom(baos.toByteArray())).build());
            }

            @Override
            public void close() {
                checkpoints.onCompleted();
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

        private class AddressSender implements AutoCloseable, Serializable {
            private final ManagedChannel managedChannel;

            public AddressSender(InetSocketAddress address) {
                LOG.info("called address sender constructor");
                managedChannel = ManagedChannelBuilder.forAddress(address.getHostName(), address.getPort())
                        .usePlaintext().intercept(new ClientInterceptor() {
                            @Override
                            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                                    MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next
                            ) {
                                return new ForwardingClientCall.SimpleForwardingClientCall<>(
                                        next.newCall(method, callOptions)
                                ) {};
                            }
                        }).build();
            }

            public void sendAddress(String address) {
                AddressServiceGrpc.newStub(managedChannel).sendAddress(
                        Services.Address.newBuilder().setAddress(address).build(),
                        new StreamObserver<>() {
                            @Override
                            public void onNext(Empty value) {
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
                            }
                        }
                );
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
}
