package com.flamestream.optimizer.sql.agents.source;

import com.flamestream.optimizer.sql.agents.Services;
import com.flamestream.optimizer.sql.agents.WorkerServiceGrpc;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

// TODO fix nullability annotations, remove all other annotations
public class SourceWrapper<T, U extends UnboundedSource.CheckpointMark> extends UnboundedSource<T, U> {
    public static final Metadata.Key<String> TAG = Metadata.Key.of("tag", Metadata.ASCII_STRING_MARSHALLER);

    public static final Logger LOG = LoggerFactory.getLogger("optimizer.source");

    private final int portNumber;

    private final UnboundedSource<T, U> source;

    public SourceWrapper(final UnboundedSource<T, U> source) {
        this.source = source;
        this.portNumber = ThreadLocalRandom.current().nextInt(9000, 65000);
        LOG.info("chose port number " + this.portNumber);
    }

    public int getPortNumber() {
        return portNumber;
    }

    @Override
    public List<SourceWrapper<T, U>> split(int desiredNumSplits, PipelineOptions options) throws Exception {
        LOG.info("split");
//        return List.of(new SourceWrapper<>(source));
        return List.of(this);
    }

    @Override
    public @NonNull UnboundedReader<T> createReader(@NonNull PipelineOptions options,
                                                    @Nullable U checkpointMark) throws IOException {
        LOG.info("created reader");
        return new ReaderWrapper(source.createReader(options, checkpointMark));
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
        private final NIOServer server;

        private ReaderWrapper(final UnboundedReader<T> reader) throws IOException {
            this.reader = reader;
            LOG.info("started worker server on source on port " + portNumber);
            server = new NIOServer(portNumber);
        }

        @Override
        public boolean start() throws IOException {
            return reader.start();
        }

        @Override
        public boolean advance() throws IOException {
            final boolean res = reader.advance();
            if (server.result != null) {
                final Instant current = getCurrentTimestamp();
                if (server.result.equals("Pause")) {
                    // TODO pause actually needs to return the max watermark that the executor agreed upon
                    /*if (current.getMillis() > server.watermark.getMillis()) {
                        return false;
                    }*/
                    return false;
                }
                if (server.result.equals("Resume")) {
                    boolean check = current.getMillis() < server.watermark.getMillis();
                    LOG.info("resume condition (advance): " + current.getMillis() + " < " + server.watermark.getMillis() + " is " + check);
                    if (check) {
                        return false;
                    }
                }
            }
            return res;
        }

        @Override
        public T getCurrent() throws NoSuchElementException {
            LOG.info("get current");
            final T res = reader.getCurrent();
            if (server.result != null) {
                final Instant current = getCurrentTimestamp();
                if (server.result.equals("Pause")) {
                    if (current.getMillis() > server.watermark.getMillis()) {
                        return null;
                    }
                }
                if (server.result.equals("Resume")) {
                    boolean check = current.getMillis() < server.watermark.getMillis();
                    LOG.info("resume condition (getCurrent): " + current.getMillis() + " < " + server.watermark.getMillis() + " is " + check);
                    if (check) {
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
            LOG.info("element " + text);*/
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

        private class NIOServer implements AutoCloseable {
            private final Server server;
            private Instant watermark;

            public NIOServer(int port) throws IOException {
                final var targetKey = Context.key("target");
                final var workerKey = Context.key("worker");
                this.server = ServerBuilder.forPort(port).intercept(new ServerInterceptor() {
                    @Override
                    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next
                    ) {
                        LOG.info("call intercept actually works");
                        return Contexts.interceptCall(
                                Context.current(),
                                call, headers, next
                        );
                    }
                }).addService(new WorkerServiceGrpc.WorkerServiceImplBase() {
                    @Override
                    public void pause(Services.Empty request, StreamObserver<Services.Timestamp> responseObserver) {
                        LOG.info("pause called on server on source");
                        result = "Pause"; // Looks Bad
                        responseObserver.onNext(Services.Timestamp.newBuilder().setValue(getWatermark().getMillis()).build());
                    }

                    @Override
                    public void resumeTo(Services.Timestamp request, StreamObserver<Services.Empty> responseObserver) {
                        LOG.info("resume called on server on source");
                        result = "Resume";
                        watermark = Instant.ofEpochMilli(request.getValue());
                        responseObserver.onNext(Services.Empty.newBuilder().getDefaultInstanceForType());
                    }
                }).build();
                LOG.info("started worker server");
                server.start();
            }

            public String result;

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
    }
}
