package ru.finam.borsch.partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.InetAddress;
import ru.finam.borsch.cluster.MemberListener;
import ru.finam.borsch.rpc.client.BorschClientManager;


/**
 * Member listener
 * Created by akhaymovich on 18.09.17.
 */
public class MemberListenerImpl implements MemberListener {

    private static final Logger LOG = LoggerFactory.getLogger(MemberListenerImpl.class);

    private final BorschClientManager borschClientManager;
    private final ConsistentHashRing consistentHashRing;


    public MemberListenerImpl(BorschClientManager borschClientManager,
                              ConsistentHashRing consistentHashRing) {
        this.borschClientManager = borschClientManager;
        this.consistentHashRing = consistentHashRing;
    }

    @Override
    public void onJoin(InetAddress grpcAddress) {
        consistentHashRing.addNewServer(grpcAddress);
        borschClientManager.onAddingNewServer(grpcAddress);
        LOG.info("{}  joined cluster ", grpcAddress);
    }

    @Override
    public void inLeave(InetAddress grpcAddress) {
        borschClientManager.onShutdownServer(grpcAddress);
        consistentHashRing.removeServer(grpcAddress);
        LOG.info("{} left cluster ", grpcAddress);
    }
}
