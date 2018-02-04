package ru.finam.borsch.partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.HostPortAddress;

import java.util.*;


/**
 * Server partitioner
 * Created by akhaymovich on 14.09.17.
 */
public class ServerDistributionHolder {

    private static final Logger LOG = LoggerFactory.getLogger(ServerDistributionHolder.class);

    private final HostPortAddress ownAddress;

    private SortedSet<HostPortAddress> addressSet = new TreeSet<>();
    private HashingRing hashingRing;
    private Object addressLock = new Object();

    public ServerDistributionHolder(HostPortAddress ownAddress,
                                    Set<HostPortAddress> addressList) {
        addressSet.add(ownAddress);
        addressSet.addAll(addressList);
        this.ownAddress = ownAddress;
        this.hashingRing = new HashingRing(addressSet);
        LOG.info("Hash ring initialized. Num of servers : {}   {}", addressSet.size(), addressSet);
    }


    void onJoin(HostPortAddress hostPortAddress) {
        LOG.info("Add new server {}", hostPortAddress);
        synchronized (addressLock) {
            LOG.info("Address set {} ", addressSet.toString());
            if (!addressSet.contains(hostPortAddress)) {
                addressSet.add(hostPortAddress);
                hashingRing = new HashingRing(addressSet);
            }
        }
    }

    void onLeave(HostPortAddress hostPortAddress) {
        LOG.info("Server leave {}", hostPortAddress);
        if (hostPortAddress.equals(ownAddress)) {
            return;
        }
        synchronized (addressLock) {
            addressSet.remove(hostPortAddress);
            hashingRing = new HashingRing(addressSet);
        }
    }

    public boolean isMyData(String shard) {
        HostPortAddress serverHolder;
        synchronized (hashingRing) {
            serverHolder = hashingRing.getServer(shard);
        }
        return serverHolder.equals(ownAddress);
    }

    public int numOfMembers() {
        return addressSet.size();
    }
}
