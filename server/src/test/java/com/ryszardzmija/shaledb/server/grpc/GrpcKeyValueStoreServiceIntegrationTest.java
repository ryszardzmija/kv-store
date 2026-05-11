package com.ryszardzmija.shaledb.server.grpc;

import com.google.protobuf.ByteString;
import com.ryszardzmija.shaledb.api.v1.*;
import com.ryszardzmija.shaledb.storage.KeyValueStore;
import com.ryszardzmija.shaledb.storage.config.StorageConfig;
import com.ryszardzmija.shaledb.storage.durability.DurabilityMode;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

class GrpcKeyValueStoreServiceIntegrationTest {
    @TempDir
    Path tempDir;

    KeyValueStore store;
    ExecutorService storageExecutor;
    Server server;
    ManagedChannel channel;
    KeyValueStoreGrpc.KeyValueStoreBlockingStub client;

    @BeforeEach
    void setUp() throws IOException {
        Path segmentDir = tempDir.resolve("segments");
        Files.createDirectories(segmentDir);

        store = new KeyValueStore(new StorageConfig(
                65536,
                16384,
                segmentDir,
                DurabilityMode.SYNC_EACH_WRITE
        ));

        storageExecutor = Executors.newSingleThreadExecutor();

        server = NettyServerBuilder
                .forPort(0)
                .addService(new GrpcKeyValueStoreService(storageExecutor, store))
                .build()
                .start();

        channel = ManagedChannelBuilder
                .forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();

        client = KeyValueStoreGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        channel.shutdownNow();
        boolean channelTerminated = channel.awaitTermination(5, TimeUnit.SECONDS);
        assertThat(channelTerminated).isTrue();

        server.shutdownNow();
        boolean serverTerminated = server.awaitTermination(5, TimeUnit.SECONDS);
        assertThat(serverTerminated).isTrue();

        storageExecutor.shutdownNow();
        boolean storageExecutorTerminated = storageExecutor.awaitTermination(5, TimeUnit.SECONDS);
        assertThat(storageExecutorTerminated).isTrue();

        store.close();
    }

    @Test
    void storesRetrievesAndDeletesValuesOverGrpc() {
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFrom(new byte[] {1, 2, 3});

        PutResponse putResponse = client.put(PutRequest.newBuilder()
                .setKey(key)
                .setValue(value)
                .build());
        assertThat(putResponse).isEqualTo(PutResponse.getDefaultInstance());

        GetResponse stored = client.get(GetRequest.newBuilder()
                .setKey(key)
                .build());

        assertThat(stored.getFound()).isTrue();
        assertThat(stored.getValue()).isEqualTo(value);

        DeleteResponse deleteResponse = client.delete(DeleteRequest.newBuilder()
                .setKey(key)
                .build());
        assertThat(deleteResponse).isEqualTo(DeleteResponse.getDefaultInstance());

        GetResponse deleted = client.get(GetRequest.newBuilder()
                .setKey(key)
                .build());

        assertThat(deleted.getFound()).isFalse();
        assertThat(deleted.getValue()).isEqualTo(ByteString.empty());
    }

    private static <T> void assertRejectsEmptyKey(Callable<T> rpcCall) {
        assertThatExceptionOfType(StatusRuntimeException.class)
                .isThrownBy(() -> {
                    T response = rpcCall.call();
                    fail("Expected get to fail, but returned: " + response);
                })
                .satisfies(exception -> {
                    Status status = Status.fromThrowable(exception);

                    assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(status.getDescription()).isEqualTo("key must not be empty");
                });
    }

    @Test
    void rejectsGetWithEmptyKey() {
        assertRejectsEmptyKey(() -> client.get(GetRequest.getDefaultInstance()));
    }

    @Test
    void rejectsPutWithEmptyKey() {
        assertRejectsEmptyKey(() -> client.put(PutRequest.getDefaultInstance()));
    }

    @Test
    void rejectsDeleteWithEmptyKey() {
        assertRejectsEmptyKey(() -> client.delete(DeleteRequest.getDefaultInstance()));
    }
}
