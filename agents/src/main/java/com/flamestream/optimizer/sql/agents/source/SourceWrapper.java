package com.flamestream.optimizer.sql.agents.source;

import com.flamestream.optimizer.sql.agents.Executor;
import com.flamestream.optimizer.sql.agents.Services;
import com.flamestream.optimizer.sql.agents.WorkerServiceGrpc;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p26p0.org.bouncycastle.util.Times;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

// TODO fix nullability annotations, remove all other annotations
public class SourceWrapper extends UnboundedSource<Row, UnboundedSource.CheckpointMark> {
    public static final Metadata.Key<String> TAG = Metadata.Key.of("tag", Metadata.ASCII_STRING_MARSHALLER);

    private final UnboundedSource<Row, CheckpointMark> source;

    public SourceWrapper(final UnboundedSource<Row, UnboundedSource.CheckpointMark> source) {
        this.source = source;
    }

    @Override
    public List<? extends UnboundedSource<Row, CheckpointMark>> split(int desiredNumSplits, PipelineOptions options) throws Exception {
        return source.split(desiredNumSplits, options).stream()
                .map(SourceWrapper::new)
                .collect(Collectors.toList());
    }

    @Override
    public @NonNull UnboundedReader<Row> createReader(@NonNull PipelineOptions options,
                                                      @Nullable CheckpointMark checkpointMark) throws IOException {
        return new ReaderWrapper(source.createReader(options, checkpointMark));
    }

    @Override
    public @NonNull Coder<CheckpointMark> getCheckpointMarkCoder() {
        return source.getCheckpointMarkCoder();
    }

    private class ReaderWrapper extends UnboundedReader<Row> {
        private final UnboundedReader<Row> reader;
        private final NIOServer server;

        // TODO visibility
        public ReaderWrapper(final UnboundedReader<Row> reader) {
            this.reader = reader;
            this.server = new NIOServer(9000);
            this.server.start();
        }

        @Override
        public boolean start() throws IOException {
            return reader.start();
        }

        @Override
        public boolean advance() throws IOException {
            if (this.server.result.equals("Pause")) {
                return false;
            }
            // don't actually start until we reached the correct timestamp?
            if (this.server.result.equals("Resume") && getCurrentTimestamp().getMillis() < server.timestamp) {
                return true;
            }
            return reader.advance();
        }

        @Override
        public Row getCurrent() throws NoSuchElementException {
            if (!this.server.result.equals("Resume") && getCurrentTimestamp().getMillis() < server.timestamp) {
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
        public @NonNull UnboundedSource<Row, ?> getCurrentSource() {
            return reader.getCurrentSource();
        }
    }

    // TODO copied over from StatisticsHandling, we'll need to generalize this somehow
    public static class NIOServer {
        private final int port;

        public NIOServer(int port) {
            this.port = port;
        }
        public String result;
        public long timestamp = -1;

        public void start() {
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
            }).addService(new WorkerServiceGrpc.WorkerServiceImplBase() {
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
}
