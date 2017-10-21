package ru.finam.borsch.partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.HostPortAddress;
import ru.finam.borsch.cluster.MemberListener;
import ru.finam.borsch.rpc.client.BorschClientManager;


/**
 * Member listener
 * Created by akhaymovich on 18.09.17.
 */
public class MemberListenerImpl implements MemberListener {

    private static final Logger LOG = LoggerFactory.getLogger(MemberListenerImpl.class);

    private final BorschClientManager borschClientManager;
    private final ServerHolder serverHolder;


    public MemberListenerImpl(BorschClientManager borschClientManager,
                              ServerHolder serverHolder) {
        this.borschClientManager = borschClientManager;
        this.serverHolder = serverHolder;
    }

    @Override
    public void onJoin(HostPortAddress grpcAddress) {
        serverHolder.addNewServer(grpcAddress);
        borschClientManager.onAddingNewServer(grpcAddress);
        LOG.info("{}  joined cluster ", grpcAddress);
    }

    @Override
    public void onLeave(HostPortAddress grpcAddress) {
        borschClientManager.onShutdownServer(grpcAddress);
        serverHolder.removeServer(grpcAddress);
        LOG.info("{} left cluster ", grpcAddress);
    }
}
