package com.ryszardzmija.shaledb.server.grpc;

import com.google.protobuf.ByteString;
import com.ryszardzmija.shaledb.api.v1.*;
import com.ryszardzmija.shaledb.storage.KeyValueStore;
import com.ryszardzmija.shaledb.storage.StorageEngineException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

public class GrpcKeyValueStoreService extends KeyValueStoreGrpc.KeyValueStoreImplBase {
    private final ExecutorService storageExecutor;
    private final KeyValueStore store;

    public GrpcKeyValueStoreService(ExecutorService storageExecutor, KeyValueStore store) {
        this.storageExecutor = Objects.requireNonNull(storageExecutor);
        this.store = Objects.requireNonNull(store);
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        if (request.getKey().isEmpty()) {
            failInvalidKey(responseObserver);
            return;
        }

        byte[] key = request.getKey().toByteArray();

        try {
            storageExecutor.execute(() -> {
                try {
                    Optional<byte[]> value = store.get(key);

                    GetResponse response;
                    response = value.map(bytes -> GetResponse.newBuilder()
                            .setFound(true)
                            .setValue(ByteString.copyFrom(bytes))
                            .build()).orElseGet(() -> GetResponse.newBuilder()
                            .setFound(false)
                            .setValue(ByteString.empty())
                            .build());

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } catch (StorageEngineException e) {
                    failStorage(responseObserver);
                } catch (RuntimeException e) {
                    failUnexpected(responseObserver);
                }
            });
        } catch (RejectedExecutionException e) {
            failRejected(responseObserver);
        }
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        if (request.getKey().isEmpty()) {
            failInvalidKey(responseObserver);
            return;
        }

        try {
            storageExecutor.execute(() -> {
                try {
                    store.put(request.getKey().toByteArray(), request.getValue().toByteArray());
                    responseObserver.onNext(PutResponse.newBuilder().build());
                    responseObserver.onCompleted();
                } catch (StorageEngineException e) {
                    failStorage(responseObserver);
                } catch (RuntimeException e) {
                    failUnexpected(responseObserver);
                }
            });
        } catch (RejectedExecutionException e) {
            failRejected(responseObserver);
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        if (request.getKey().isEmpty()) {
            failInvalidKey(responseObserver);
            return;
        }

        try {
            storageExecutor.execute(() -> {
                try {
                    store.delete(request.getKey().toByteArray());
                    responseObserver.onNext(DeleteResponse.newBuilder().build());
                    responseObserver.onCompleted();
                } catch (StorageEngineException e) {
                    failStorage(responseObserver);
                } catch (RuntimeException e) {
                    failUnexpected(responseObserver);
                }
            });
        } catch (RejectedExecutionException e) {
            failRejected(responseObserver);
        }
    }

    private static void failInvalidKey(StreamObserver<?> responseObserver) {
        responseObserver.onError(
                Status.INVALID_ARGUMENT
                        .withDescription("key must not be empty")
                        .asRuntimeException()
        );
    }

    private static void failStorage(StreamObserver<?> responseObserver) {
        responseObserver.onError(
                Status.INTERNAL
                        .withDescription("storage engine failed")
                        .asRuntimeException()
        );
    }

    private static void failRejected(StreamObserver<?> responseObserver) {
        responseObserver.onError(
                Status.UNAVAILABLE
                        .withDescription("key value store unavailable")
                        .asRuntimeException()
        );
    }

    private static void failUnexpected(StreamObserver<?> responseObserver) {
        responseObserver.onError(
                Status.INTERNAL
                        .withDescription("unexpected server error")
                        .asRuntimeException()
        );
    }
}
