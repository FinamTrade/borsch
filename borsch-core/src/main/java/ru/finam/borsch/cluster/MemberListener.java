package ru.finam.borsch.cluster;

import ru.finam.borsch.HostPortAddress;

/**
 * Created by akhaymovich on 06.09.17.
 */
public interface MemberListener {


    void onJoin(HostPortAddress grpcAddress);

    void onLeave(HostPortAddress grpcAddress);
}
