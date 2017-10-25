package ru.finam.borsch.rpc.client;

import finam.protobuf.borsch.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.HostPortAddress;
import ru.finam.rocksdb.Store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * Client keeper
 * Created by akhaymovich on 20.09.17.
 */
public class BorschClientManager {
    private static final Logger LOG = LoggerFactory.getLogger(BorschClientManager.class);

    private final List<BorschServiceClient> activeClientList
            = new ArrayList<>();   //все клиенты кроме своего
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Store store;

    public BorschClientManager(Store store) {
        this.store = store;
    }

    public void onClusterStart(Collection<HostPortAddress> hostPortAddressList) {
        hostPortAddressList.forEach(this::onAddingNewServer);
        activeClientList.forEach(
                client -> {
                    LOG.info("Asking for snapshot from client {} ", client.getHostPortAddress());
                    client.askForSnapshot();
                }
        );
    }


    public void putToNeibours(PutRequest putRequest, Consumer<Boolean> resultListener) {
        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        try {
            activeClientList
                    .forEach(client -> client.put(putRequest, resultListener));
        } finally {
            readLock.unlock();
        }
    }

    public void onAddingNewServer(HostPortAddress newServerAddress) {
        BorschServiceClient newCient = new BorschServiceClient(newServerAddress, store);
        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        try {

            activeClientList.add(newCient);

        } finally {
            writeLock.unlock();
        }
    }

    public void onShutdownServer(HostPortAddress hostPortAddress) {
        Optional<BorschServiceClient> clientOptional =
                activeClientList.stream()
                        .filter(client -> client.getHostPortAddress().equals(hostPortAddress))
                        .findAny();
        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            clientOptional.ifPresent(activeClientList::remove);
        } finally {
            writeLock.unlock();
        }
    }


}
