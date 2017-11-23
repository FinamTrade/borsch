package ru.finam.borsch.partitioner;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.HostPortAddress;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;


class KetamaHashingRing {

    private static final Logger LOG = LoggerFactory.getLogger(KetamaHashingRing.class);
    private static final int NUM_OF_SEGMENTS = 2;
    private static final HashFunction MD5_HASH = Hashing.md5();

    private final TreeMap<Long, HostPortAddress> buckets;

    KetamaHashingRing(SortedSet<HostPortAddress> servers) {
        buckets = new TreeMap<>();
        int numOfServers = servers.size();
        for (HostPortAddress hostPortAddress : servers) {
            int thisWeight = 1;
            double factor = Math.floor(((double) (NUM_OF_SEGMENTS * numOfServers * thisWeight)) / (double) numOfServers);
            for (long j = 0; j < factor; j++) {
                byte[] d = MD5_HASH.hashString(hostPortAddress + "-" + j, Charset.defaultCharset()).asBytes();
                for (int h = 0; h < 4; h++) {
                    Long k =
                            ((long) (d[3 + h * 4] & 0xFF) << 24)
                                    | ((long) (d[2 + h * 4] & 0xFF) << 16)
                                    | ((long) (d[1 + h * 4] & 0xFF) << 8)
                                    | ((long) (d[0 + h * 4] & 0xFF));
                    buckets.put(k, hostPortAddress);
                    LOG.debug("++++ added " + hostPortAddress + " to server bucket");
                }
            }
        }
    }


    HostPortAddress getServer(String key) {
        if (buckets.isEmpty()){
            throw new RuntimeException("Invalid empty buckets");
        }
        if (buckets.size() == 1) {
            return buckets.firstEntry().getValue();
        }
        Long hv = md5KetamaAlg(key);
        Map.Entry<Long, HostPortAddress> addressEntry = buckets.ceilingEntry(hv);
        if (addressEntry == null) {
            addressEntry = buckets.firstEntry();
        }
        return addressEntry.getValue();
    }

    private static Long md5KetamaAlg(String key) {
        byte[] bKey = MD5_HASH.hashString(key, Charset.defaultCharset()).asBytes();
        long res = ((long) (bKey[3] & 0xFF) << 24)
                | ((long) (bKey[2] & 0xFF) << 16)
                | ((long) (bKey[1] & 0xFF) << 8) | (long) (bKey[0] & 0xFF);
        return res;
    }
}
