package ru.finam.borsch.launch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.BorschSettings;
import ru.finam.borsch.cluster.Cluster;
import ru.finam.borsch.cluster.consul.ConsulCluster;
import ru.finam.borsch.rpc.client.BorschClientManager;
import ru.finam.borsch.rpc.server.BorschGrpcServer;
import ru.finam.borsch.rpc.server.BorschServiceApi;
import ru.finam.rocksdb.Store;
import ru.finam.rocksdb.RocksDbStore;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Create borsch instances
 * Created by akhaymovich on 20.09.17.
 */
public class BorschFactory {

    private static final Logger LOG = LoggerFactory.getLogger(BorschFactory.class);

    static {
        System.setProperty("com.orbitz.consul.cache.backOffDelay", "0");
    }

    private final BorschGrpcServer grpcServer;
    private final Cluster cluster;
    private final ScheduledThreadPoolExecutor scheduledExecutor =
            new ScheduledThreadPoolExecutor(Math.max(8,
                    Runtime.getRuntime().availableProcessors() * 2));


    private BorschFactory(BorschSettings borschSettings) {
        Store store = new RocksDbStore(borschSettings.getPathToDb());
        BorschClientManager borschClientManager = new BorschClientManager(store);
        cluster = new ConsulCluster(borschClientManager, borschSettings, scheduledExecutor);
        BorschServiceApi borschServiceApi = new BorschServiceApi(scheduledExecutor, store, cluster, borschClientManager);
        grpcServer = new BorschGrpcServer(scheduledExecutor, borschServiceApi, cluster.grpcPort(), cluster.getHealthListener());
        LOG.info("Borsch cluster created for service {}", borschSettings.getServiceHolderId());
    }

    private void start() {
        grpcServer.start();
    }

    public static void startBorsch(BorschSettings borschSettings) {
        BorschFactory factory = new BorschFactory(borschSettings);
        factory.start();
    }

}