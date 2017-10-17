package ru.finam.borsch;


/**
 * For grpc host - port
 * Created by akhaymovich on 06.09.17.
 */
public class InetAddress {

    private final String host;
    private final int port;

    public InetAddress(String host, int port) {
        this.port = port;
        this.host = host;
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof InetAddress)) {
            return false;
        }
        InetAddress inetAddress = (InetAddress) object;
        return inetAddress.getHost().equals(host) &&
                inetAddress.getPort() == port;
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
