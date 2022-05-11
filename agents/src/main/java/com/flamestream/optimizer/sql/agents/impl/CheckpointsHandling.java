package com.flamestream.optimizer.sql.agents.impl;

import com.flamestream.optimizer.sql.agents.CheckpointServiceGrpc;
import com.flamestream.optimizer.sql.agents.Services;
import com.google.protobuf.Empty;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.io.Serializable;
import java.util.function.Function;

import static com.flamestream.optimizer.sql.agents.source.SourceWrapper.CHECKPOINT_TAG;

public class CheckpointsHandling {
    public static class NIOServer implements AutoCloseable, Serializable {
        private final Server server;

        public NIOServer(int port, Function<String, StreamObserver<Services.Checkpoint>> observer) throws IOException {
            final Context.Key<String> checkpointsKey = Context.key("checkpoints_tag");
            this.server = ServerBuilder.forPort(port).intercept(new ServerInterceptor() {
                @Override
                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next
                ) {
                    return Contexts.interceptCall(
                            Context.current().withValue(checkpointsKey, headers.get(CHECKPOINT_TAG)),
                            call, headers, next);
                }
            }).addService(new CheckpointServiceGrpc.CheckpointServiceImplBase() {
                @Override
                public StreamObserver<Services.Checkpoint> push(StreamObserver<Empty> responseObserver) {
                    final StreamObserver<Services.Checkpoint> targetObserver = observer.apply(checkpointsKey.get());
                    return new StreamObserver<>() {
                        @Override
                        public void onNext(Services.Checkpoint value) {
                            targetObserver.onNext(value);
                        }

                        @Override
                        public void onError(Throwable t) {
                            targetObserver.onError(t);
                        }

                        @Override
                        public void onCompleted() {
                            targetObserver.onCompleted();
                        }
                    };
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