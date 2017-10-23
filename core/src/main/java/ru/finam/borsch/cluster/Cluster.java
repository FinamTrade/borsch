package ru.finam.borsch.cluster;


import ru.finam.borsch.HostPortAddress;
import ru.finam.borsch.rpc.client.BorschClientManager;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by akhaymovich on 06.09.17.
 */
public abstract class Cluster implements ClusterInfo{

    protected final Map<HostPortAddress, Long> inetAddressMap = new HashMap<>();
    private final BorschClientManager borschClientManager;

    public Cluster(BorschClientManager borschClientManager) {
        this.borschClientManager = borschClientManager;
    }

    protected void synchronizeData(){
        borschClientManager.onClusterStart(inetAddressMap.keySet());
    }

    public abstract Consumer<Boolean> getHealthListener();
}
