package ru.finam.borsch.partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.HostPortAddress;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class KetamaHashingRing {

    private static final Logger LOG = LoggerFactory.getLogger(KetamaHashingRing.class);
    private static TreeMap<Long, HostPortAddress> buckets;

    public KetamaHashingRing(List<HostPortAddress> servers) {
        buckets = new TreeMap<>();
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            LOG.error("++++ no md5 algorythm found");
            throw new IllegalStateException("++++ no md5 algorythm found");
        }
        int numOfServers = servers.size();
        for (int i = 0; i < numOfServers; i++) {
            int thisWeight = 1;
            double factor = Math.floor(((double) (40 * numOfServers * thisWeight)) / (double) numOfServers);
            for (long j = 0; j < factor; j++) {
                byte[] d = md5.digest((servers.get(i) + "-" + j).getBytes());
                for (int h = 0; h < 4; h++) {
                    Long k =
                            ((long) (d[3 + h * 4] & 0xFF) << 24)
                                    | ((long) (d[2 + h * 4] & 0xFF) << 16)
                                    | ((long) (d[1 + h * 4] & 0xFF) << 8)
                                    | ((long) (d[0 + h * 4] & 0xFF));
                    buckets.put(k, servers.get(i));
                    LOG.debug("++++ added " + servers.get(i) + " to server bucket");
                }
            }
        }
    }


    public HostPortAddress getServer(String key) {
        if (buckets.size() == 1) {
            return buckets.firstEntry().getValue();
        }
        Long hv = md5HashingAlg(key);
        Map.Entry<Long, HostPortAddress> addressEntry = buckets.ceilingEntry(hv);
        if (addressEntry == null) {
            addressEntry = buckets.firstEntry();
        }
        return addressEntry.getValue();
    }

    private static Long md5HashingAlg(String key) {
        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            LOG.error("++++ no md5 algorythm found");
            throw new IllegalStateException("++++ no md5 algorythm found");
        }
        md5.reset();
        md5.update(key.getBytes());
        byte[] bKey = md5.digest();
        long res = ((long) (bKey[3] & 0xFF) << 24)
                | ((long) (bKey[2] & 0xFF) << 16)
                | ((long) (bKey[1] & 0xFF) << 8) | (long) (bKey[0] & 0xFF);
        return res;
    }
}
