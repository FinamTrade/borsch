package ru.finam.borsch;


import com.google.common.hash.Hashing;

import java.nio.charset.Charset;

/**
 * For grpc host - port
 * Created by akhaymovich on 06.09.17.
 */
public class HostPortAddress implements Comparable<HostPortAddress> {

    private final String host;
    private final int port;

    public HostPortAddress(String host, int port) {
        this.port = port;
        this.host = host;
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof HostPortAddress)) {
            return false;
        }
        HostPortAddress hostPortAddress = (HostPortAddress) object;
        return hostPortAddress.getHost().equals(host) &&
                hostPortAddress.getPort() == port;
    }

    @Override
    public int hashCode() {
        return Hashing.murmur3_32().newHasher().putString(host, Charset.defaultCharset()).putInt(port).hash().asInt();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    @Override
    public int compareTo(HostPortAddress address) {
        if (host.equals(address.getHost())) {
            return Integer.compare(port, address.getPort());
        } else {
            return host.compareTo(address.getHost());
        }
    }
}