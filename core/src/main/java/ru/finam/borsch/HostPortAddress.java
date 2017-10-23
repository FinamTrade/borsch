package ru.finam.borsch;


import java.util.Comparator;

/**
 * For grpc host - port
 * Created by akhaymovich on 06.09.17.
 */
public class HostPortAddress {

    private final String host;
    private final int port;

    public static final Comparator<HostPortAddress> PORT_ADDRESS_COMPARATOR = (address1, address2) -> {
        if (address1.getHost().equals(address2.getHost())) {
            return Integer.compare(address1.getPort(), address2.getPort());
        } else {
            return address1.getHost().compareTo(address2.getHost());
        }
    };

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
        return host.hashCode() ^ port;
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
}
