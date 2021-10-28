package com.flamestream.optimizer.sql.agents.source;

import com.flamestream.optimizer.sql.agents.Services;
import com.flamestream.optimizer.sql.agents.WorkerServiceGrpc;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

// TODO fix nullability annotations, remove all other annotations
public class SourceWrapper<T, U extends UnboundedSource.CheckpointMark> extends UnboundedSource<T, U> {
    public static final Metadata.Key<String> TAG = Metadata.Key.of("tag", Metadata.ASCII_STRING_MARSHALLER);

    private final int portNumber;

    private final UnboundedSource<T, U> source;

    public SourceWrapper(final UnboundedSource<T, U> source) {
        this.source = source;
        this.portNumber = ThreadLocalRandom.current().nextInt(9000, 65000);;
    }

    public int getPortNumber() {
        return portNumber;
    }

    @Override
    public List<SourceWrapper<T, U>> split(int desiredNumSplits, PipelineOptions options) throws Exception {
        System.out.println("split");
        return source.split(1, options).stream().map(SourceWrapper::new).collect(Collectors.toList());
//        return List.of(this);
        /*return source.split(desiredNumSplits, options).stream()
                .map(SourceWrapper::new)
                .collect(Collectors.toList());*/
    }

    @Override
    public @NonNull UnboundedReader<T> createReader(@NonNull PipelineOptions options,
                                                    @Nullable U checkpointMark) throws IOException {
        System.out.println("create reader");
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

        private ReaderWrapper(final UnboundedReader<T> reader) {
            this.reader = reader;
            this.server = new NIOServer();
            this.server.start();
        }

        @Override
        public boolean start() throws IOException {
            return reader.start();
        }

        @Override
        public boolean advance() throws IOException {
            if (this.server.result != null && this.server.result.equals("Pause")) {
                return false;
            }
            // don't actually start until we reached the correct timestamp?
            // TODO what was the point of this check...
            /*if (this.server.result != null && this.server.result.equals("Resume") && getCurrentTimestamp().getMillis() < server.timestamp) {
                return true;
            }*/
            return reader.advance();
        }

        @Override
        public T getCurrent() throws NoSuchElementException {
            // TODO current timestamp check?
            if (this.server.result != null && this.server.result.equals("Pause")) {
                return null;
            }
            return reader.getCurrent();
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
            final Instant watermark = reader.getWatermark();
            server.timestamp = watermark.getMillis();
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
    }

    public class NIOServer implements AutoCloseable {
        public String result;
        public long timestamp = -1;
        private Server server;

        public void start() {
            System.out.println("Started server on port " + portNumber);
            final var tag = Context.key("user-agent");
            server = ServerBuilder.forPort(portNumber)
              /*      .intercept(new ServerInterceptor() {
                @Override
                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next
                ) {
                    return Contexts.interceptCall(
                            Context.current().withValue(tag, headers.get(TAG)).withValue(Context.key("worker"), port),
                            call, headers, next
                    );
                }
            })*/
                    .addService(new WorkerServiceGrpc.WorkerServiceImplBase() {
                @Override
                public void pause(Services.Empty request, StreamObserver<Services.Timestamp> responseObserver) {
                    result = "Pause";
                    responseObserver.onNext(Services.Timestamp.newBuilder().setValue(timestamp).build());
                    responseObserver.onCompleted();
                    log("Pause");
                }

                @Override
                public void resumeTo(Services.Timestamp request, StreamObserver<Services.Empty> responseObserver) {
                    result = "Resume";
                    timestamp = request.getValue();
                    // TODO or is it
                    responseObserver.onCompleted();
                    log("Resume");
                }
            }).build();
            System.out.println("on server: " + portNumber);
        }

        private void log(String str) {
            System.out.println(str);
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
