package ru.finam.borsch.rpc.client;

import finam.protobuf.borsch.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.HostPortAddress;
import ru.finam.rocksdb.Store;

import java.util.*;
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

    public void onClusterStart(long sinceMillis) {
        LOG.info("Acsking for updates since ", new Date(sinceMillis));
        askForSnapshotFrom(0, sinceMillis);
    }

    private void askForSnapshotFrom(int clientNumber, long sinceMillis) {
        if (clientNumber >= activeClientList.size()) {
            return;
        }
        BorschServiceClient serviceClient = activeClientList.get(clientNumber);
        serviceClient.askForSnapshot(aVoid -> askForSnapshotFrom(clientNumber + 1, sinceMillis), sinceMillis);
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
            if (!activeClientList.contains(newServerAddress)) {
                activeClientList.add(newCient);
            }
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
