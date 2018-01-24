package ru.finam.borsch;


import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.util.Comparator;

/**
 * For grpc host - grpcBorschPort
 * Created by akhaymovich on 06.09.17.
 */
public class HostPortAddress implements Comparable<HostPortAddress> {

    private static final Comparator<HostPortAddress> HOSTS_COMPARATOR =
            Comparator
                    .comparing(HostPortAddress::getHost)
                    .thenComparing(HostPortAddress::getGrpcBorschPort);

    private final String host;
    private final int grpcBorschPort;
    private final int hashRingPort;

    public HostPortAddress(String host, int grpcBorschPort, int hashRingPort) {
        this.grpcBorschPort = grpcBorschPort;
        this.host = host;
        this.hashRingPort = hashRingPort;
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof HostPortAddress)) {
            return false;
        }
        HostPortAddress hostPortAddress = (HostPortAddress) object;
        return hostPortAddress.getHost().equals(host) &&
                hostPortAddress.getGrpcBorschPort() == grpcBorschPort;
    }

    @Override
    public int hashCode() {
        return Hashing.murmur3_32().newHasher().putString(host, Charset.defaultCharset()).putInt(grpcBorschPort).hash().asInt();
    }

    public String getHost() {
        return host;
    }

    public int getGrpcBorschPort() {
        return grpcBorschPort;
    }

    @Override
    public String toString() {
        return host + ":" + grpcBorschPort;
    }

    public int getHashRingPort() {
        return hashRingPort;
    }

    @Override
    public int compareTo(HostPortAddress address) {
        return HOSTS_COMPARATOR.compare(this, address);
    }
}
