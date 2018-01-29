package ru.finam.borsch.partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.HostPortAddress;
import ru.finam.borsch.cluster.MemberListener;
import ru.finam.borsch.rpc.client.BorschClientManager;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Member listener
 * Created by akhaymovich on 18.09.17.
 */
public class MemberListenerImpl implements MemberListener {

    private static final Logger LOG = LoggerFactory.getLogger(MemberListenerImpl.class);

    private final BorschClientManager borschClientManager;
    private final ServerDistributionHolder serverDistributionHolder;
    private final List<Runnable> actionList;


    public MemberListenerImpl(BorschClientManager borschClientManager,
                              List<Runnable> actionList,
                              ServerDistributionHolder serverDistributionHolder) {
        this.borschClientManager = borschClientManager;
        this.serverDistributionHolder = serverDistributionHolder;
        this.actionList = actionList;
    }

    @Override
    public void onJoin(HostPortAddress grpcAddress) {
        serverDistributionHolder.onJoin(grpcAddress);
        borschClientManager.onAddingNewServer(grpcAddress);
        createChangesWithSync();
        LOG.info("{}  joined cluster ", grpcAddress);
    }

    @Override
    public void onLeave(HostPortAddress grpcAddress) {
        borschClientManager.onShutdownServer(grpcAddress);
        serverDistributionHolder.onLeave(grpcAddress);
        createChangesWithoutSync();
        LOG.info("{} left cluster ", grpcAddress);
    }

    private CompletableFuture<Void> createChangesWithSync() {
        return CompletableFuture.runAsync(actionList.get(0))
                .thenRun(actionList.get(1))
                .thenRun(actionList.get(2));
    }

    private CompletableFuture<Void> createChangesWithoutSync() {
        return CompletableFuture.runAsync(actionList.get(0))
                .thenRun(actionList.get(2));
    }
}
