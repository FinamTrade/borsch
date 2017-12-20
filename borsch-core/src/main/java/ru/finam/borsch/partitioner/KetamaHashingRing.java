package ru.finam.borsch.partitioner;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import net.openhft.hashing.LongHashFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.HostPortAddress;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;


class KetamaHashingRing {

    private static final Logger LOG = LoggerFactory.getLogger(KetamaHashingRing.class);
    private static final int MIN_RING_SIZE = 1024;

    private final TreeMap<Long, HostPortAddress> buckets;
    private final Object lock = new Object();

    KetamaHashingRing(SortedSet<HostPortAddress> servers) {
        LOG.info("Counting ring for number of servers {}", servers.size());
        synchronized (lock) {
            buckets = new TreeMap<>();
            int numOfServers = servers.size();
            double factor = getHashesPerHost(numOfServers);
            for (HostPortAddress hostPortAddress : servers) {
                for (long j = 0; j < factor; j++) {
                    long serverPartId = hash(hostPortAddress.toString() + "_" + String.valueOf(j));
                    buckets.put(serverPartId, hostPortAddress);
                }
            }
        }
        LOG.info("Num of buckets {}", buckets.size());
    }

//    KetamaHashingRing(SortedSet<HostPortAddress> servers) {
//        buckets = new TreeMap<>();
//        int numOfServers = servers.size();
//        int hashPerHost = getHashesPerHost(numOfServers);
//        for (HostPortAddress hostPortAddress : servers) {
//            String stringForHash = hostPortAddress.getHost() + hostPortAddress.getPort();
//            for (long j = 0; j < hashPerHost; j++) {
//                long hash = hash(stringForHash +"_" + String.valueOf(j) );
//
//
//                buckets.put(hash, hostPortAddress);
//                LOG.debug("added  {}  to server bucket ", hostPortAddress);
//            }
//        }
//    }


    HostPortAddress getServer(String key) {
        synchronized (lock) {
            if (buckets.isEmpty()) {
                throw new RuntimeException("Invalid empty buckets");
            }
            if (buckets.size() == 1) {
                return buckets.firstEntry().getValue();
            }
            Long hv = hash(key);
            Map.Entry<Long, HostPortAddress> addressEntry = buckets.ceilingEntry(hv);
            if (addressEntry == null) {
                addressEntry = buckets.firstEntry();
            }
            return addressEntry.getValue();
        }
    }

    private static int getHashesPerHost(int numOfHosts) {
        int hashesPerHost = 1;
        if (numOfHosts < MIN_RING_SIZE) {
            hashesPerHost = MIN_RING_SIZE / numOfHosts;
            if ((MIN_RING_SIZE % numOfHosts) != 0) {
                hashesPerHost++;
            }
        }
        LOG.info("Min ring size {} , hashes per host {}", MIN_RING_SIZE, hashesPerHost);
        return hashesPerHost;
    }

    private static long hash(String key) {
        return LongHashFunction.xx().hashBytes(key.getBytes());
    }

}
