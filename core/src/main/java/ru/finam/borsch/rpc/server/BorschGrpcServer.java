package ru.finam.borsch.rpc.server;

import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Grpc server
 * Created by akhaymovich on 14.09.17.
 */
public class BorschGrpcServer {

    private static final Logger LOG = LoggerFactory.getLogger(BorschGrpcServer.class);
    private final ExecutorService executor;
    private final int port;
    private final Consumer<Boolean> healthConsumer;
    private final Server server;

    public BorschGrpcServer(BorschServiceApi serviceApi,
                            int port,
                            Consumer<Boolean> healthConsumer) {
        this.executor = Executors.newFixedThreadPool(
                Math.max(8, Runtime.getRuntime().availableProcessors() * 2));
        this.server = createServer(serviceApi, port);
        this.port = port;
        this.healthConsumer = healthConsumer;
    }


    private void stop() {
        healthConsumer.accept(false);
        server.shutdown();
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }


    private Server createServer(BorschServiceApi serviceApi, int port) {

        CompressorRegistry registry = CompressorRegistry
                .newEmptyInstance();
        registry.register(new Codec.Gzip());
        return NettyServerBuilder.forPort(port)
                .bossEventLoopGroup(new NioEventLoopGroup(1, executor))
                .workerEventLoopGroup(new NioEventLoopGroup(4, executor))
                .executor(executor)
                .compressorRegistry(registry)
                .addService(serviceApi)
                .build();
    }

    public void start() {
        try {
            server.start();
            healthConsumer.accept(true);
            LOG.info("Grpc is listening on port {} ", port);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("*** shutting down gRPC server since JVM is shutting down");
                BorschGrpcServer.this.stop();
                LOG.info("*** server shut down");
            }));
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }

}
