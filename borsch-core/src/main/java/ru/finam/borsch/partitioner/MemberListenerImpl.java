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
    private final List<Runnable> actionListOnChange;


    public MemberListenerImpl(BorschClientManager borschClientManager,
                              List<Runnable> actionListOnChange,
                              ServerDistributionHolder serverDistributionHolder) {
        this.borschClientManager = borschClientManager;
        this.serverDistributionHolder = serverDistributionHolder;
        this.actionListOnChange = actionListOnChange;
    }

    @Override
    public void onJoin(HostPortAddress grpcAddress) {
        serverDistributionHolder.onJoin(grpcAddress);
        borschClientManager.onAddingNewServer(grpcAddress);
        createChangesChain();
        LOG.info("{}  joined cluster ", grpcAddress);
    }

    @Override
    public void onLeave(HostPortAddress grpcAddress) {
        borschClientManager.onShutdownServer(grpcAddress);
        serverDistributionHolder.onLeave(grpcAddress);
        createChangesChain();
        LOG.info("{} left cluster ", grpcAddress);
    }

    private CompletableFuture<Void> createChangesChain() {
        return CompletableFuture.runAsync(actionListOnChange.get(0))
                .thenRun(actionListOnChange.get(1))
                .thenRun(actionListOnChange.get(2));
    }
}
