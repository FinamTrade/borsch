package ru.finam.borsch;


/**
 * For grpc host - port
 * Created by akhaymovich on 06.09.17.
 */
public class HostPortAddress {

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
