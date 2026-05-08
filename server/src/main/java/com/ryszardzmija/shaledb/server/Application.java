package com.ryszardzmija.shaledb.server;

import com.ryszardzmija.shaledb.server.config.ApplicationConfigDto;
import com.ryszardzmija.shaledb.server.config.ApplicationConfigLoader;
import com.ryszardzmija.shaledb.server.grpc.GrpcKeyValueStoreService;
import com.ryszardzmija.shaledb.storage.KeyValueStore;
import com.ryszardzmija.shaledb.storage.config.StorageConfig;
import com.ryszardzmija.shaledb.storage.config.StorageConfigMapper;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);
    private static final int GRPC_PORT = 50001;

    void main(String[] args) {
        String configFile = "config/application.yaml";
        if (args.length > 0) {
            configFile = args[0];
        }

        ApplicationConfigLoader applicationConfigLoader = ApplicationConfigLoader.fromYAML();
        ApplicationConfigDto applicationConfig = applicationConfigLoader.loadFromFile(Path.of(configFile));

        StorageConfigMapper storageConfigMapper = new StorageConfigMapper();
        StorageConfig storageConfig = storageConfigMapper.toStorageConfig(applicationConfig.storage());

        try {
            Files.createDirectories(storageConfig.segmentDir());
            try (var store = new KeyValueStore(storageConfig);
                 var storageExecutor = Executors.newSingleThreadExecutor()) {

                Server server = NettyServerBuilder
                        .forPort(GRPC_PORT)
                        .addService(new GrpcKeyValueStoreService(storageExecutor, store))
                        .build()
                        .start();

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    logger.info("Shutting down ShaleDB gRPC server");
                    server.shutdown();
                    storageExecutor.shutdown();
                }));

                server.awaitTermination();
            }
        } catch (IOException e) {
            logger.error("Failed to start ShaleDB server", e);
            System.exit(1);
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Server interrupted", e);
            System.exit(1);
        } catch (RuntimeException e) {
            logger.error("Fatal error during execution", e);
            System.exit(1);
        }
    }
}
