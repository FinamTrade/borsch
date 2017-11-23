package ru.finam.borsch.partitioner;


import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.HostPortAddress;
import ru.finam.borsch.rpc.server.BorschServiceApi;
import java.util.*;


/**
 * Server partitioner
 * Created by akhaymovich on 14.09.17.
 */
public class ServerDistributionHolder {

    private static final Logger LOG = LoggerFactory.getLogger(BorschServiceApi.class);

    private final HostPortAddress ownAddress;

    private SortedSet<HostPortAddress> addressSet = new TreeSet<>();
    private KetamaHashingRing hashingRing;
    private Object addressLock = new Object();

    public ServerDistributionHolder(HostPortAddress ownAddress,
                                    List<HostPortAddress> addressList) {
        addressSet.add(ownAddress);
        addressSet.addAll(addressList);
        this.ownAddress = ownAddress;
        this.hashingRing = new KetamaHashingRing(addressSet);
        LOG.info("Hash ring initialized. Num of servers : {}", addressList.size());
    }


    void onJoin(HostPortAddress hostPortAddress) {
        LOG.info("Add new server {}", hostPortAddress);
        synchronized (addressLock) {
            if (!addressSet.contains(hostPortAddress)) {
                addressSet.add(hostPortAddress);
                hashingRing = new KetamaHashingRing(addressSet);
            }
        }
    }

    void onLeave(HostPortAddress hostPortAddress) {
        LOG.info("Server leave {}", hostPortAddress);
        if (hostPortAddress.equals(ownAddress)){
            return;
        }
        synchronized (addressLock) {
            addressSet.remove(hostPortAddress);
            hashingRing = new KetamaHashingRing(addressSet);
        }
    }

    public boolean isMyData(ByteString accountHash) {
        HostPortAddress serverHolder = hashingRing.getServer(new String(accountHash.toByteArray()));
        return serverHolder.equals(ownAddress);
    }

    public int numOfMembers() {
        return addressSet.size();
    }
}
