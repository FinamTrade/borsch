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

/**
 * Create borsch instances
 * Created by akhaymovich on 20.09.17.
 */
class BorschFactory {

    private static final Logger LOG = LoggerFactory.getLogger(BorschFactory.class);

    private final BorschGrpcServer grpcServer;
    private final Cluster cluster;


    BorschFactory(BorschSettings borschSettings) {
        Store store = new RocksDbStore(borschSettings.getPathToDb());
        BorschClientManager borschClientManager = new BorschClientManager(store);
        cluster = new ConsulCluster(borschClientManager,
                borschSettings);
        BorschServiceApi borschServiceApi = new BorschServiceApi(store, cluster, borschClientManager);
        grpcServer = new BorschGrpcServer(borschServiceApi, cluster.grpcPort(), cluster.getHealthListener());
        LOG.info("Borsch cluster created for service {}", borschSettings.getServiceHolderId());
    }

    void run() {
        LOG.info("Start grpc");
        grpcServer.start();
        LOG.info("Start cluster");
        cluster.start();
    }

}
