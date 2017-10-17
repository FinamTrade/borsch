package ru.finam.borsch.cluster;

import ru.finam.borsch.InetAddress;

/**
 * Created by akhaymovich on 06.09.17.
 */
public interface MemberListener {


    void onJoin(InetAddress grpcAddress);

    void inLeave(InetAddress grpcAddress);
}
