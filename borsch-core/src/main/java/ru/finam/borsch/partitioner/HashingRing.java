package ru.finam.borsch.partitioner;

import net.openhft.hashing.LongHashFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.HostPortAddress;

import java.math.BigInteger;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;


class HashingRing {

    private static final Logger LOG = LoggerFactory.getLogger(HashingRing.class);
    private static final int MIN_RING_SIZE = 1024;

    private final TreeMap<BigInteger, HostPortAddress> buckets;
    private final Object lock = new Object();

    private static final BigInteger BI_2_64 = BigInteger.ONE.shiftLeft(64);

    public static String asString(long l) {
        return l >= 0 ? String.valueOf(l) : toBigInteger(l).toString();
    }

    public static BigInteger toBigInteger(long l) {
        final BigInteger bi = BigInteger.valueOf(l);
        return l >= 0 ? bi : bi.add(BI_2_64);
    }

    HashingRing(SortedSet<HostPortAddress> servers) {
        LOG.info("Counting ring for number of servers {}", servers.size());
        for (HostPortAddress address : servers){
            LOG.info("HostPortAddress : {} ", address.toString());
        }
        synchronized (lock) {
            buckets = new TreeMap<>();
            int numOfServers = servers.size();
            double factor = getHashesPerHost(numOfServers);
            for (HostPortAddress hostPortAddress : servers) {
                for (long j = 0; j < factor; j++) {
                    String addressStr = hostPortAddress.getHost() + ":" + hostPortAddress.getHashRingPort() + "_" + String.valueOf(j);
                    BigInteger serverPartId = hash(addressStr);
                    buckets.put(serverPartId, hostPortAddress);
                }
            }
        }
        LOG.info("Num of buckets {}", buckets.size());
    }

    HostPortAddress getServer(String key) {
        synchronized (lock) {
            if (buckets.isEmpty()) {
                throw new RuntimeException("Invalid empty buckets");
            }
            if (buckets.size() == 1) {
                return buckets.firstEntry().getValue();
            }
            BigInteger hv = hash(key);
            Map.Entry<BigInteger, HostPortAddress> addressEntry = buckets.ceilingEntry(hv);

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

    private static BigInteger hash(String key) {
        return toBigInteger(LongHashFunction.xx(0).hashBytes(key.getBytes()));
    }

    public static void main(String[] args) {
        long var3 = LongHashFunction.xx(0).hashBytes("5d1239bc-4551-435e-a93a-e428c47f831e".getBytes());
        System.out.println(asString(var3));
    }

}
