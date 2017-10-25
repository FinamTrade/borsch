package ru.finam.borsch.rpc.client;

import com.google.protobuf.Timestamp;
import finam.protobuf.borsch.*;
import io.grpc.ManagedChannel;
import io.grpc.okhttp.OkHttpChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.HostPortAddress;
import ru.finam.rocksdb.Store;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * CLient of grpc server
 * Created by akhaymovich on 20.09.17.
 */
public class BorschServiceClient {

    private static final Logger LOG = LoggerFactory.getLogger(BorschServiceClient.class);

    private final BorschServiceGrpc.BorschServiceStub serviceStub;
    private final HostPortAddress hostPortAddress;
    private final Store store;

    BorschServiceClient(HostPortAddress hostPortAddress, Store store) {
        ManagedChannel managedChannel = OkHttpChannelBuilder.forAddress(hostPortAddress.getHost(), hostPortAddress.getPort())
                .usePlaintext(true)
                .idleTimeout(1, TimeUnit.MINUTES)
                .build();
        serviceStub = BorschServiceGrpc.newStub(managedChannel);
        this.hostPortAddress = hostPortAddress;
        this.store = store;
    }


    void put(PutRequest putRequest, Consumer<Boolean> resultConsumer) {

        serviceStub.put(putRequest, new StreamObserver<PutResponse>() {
            @Override
            public void onNext(PutResponse value) {
                resultConsumer.accept(value.getResult());
            }

            @Override
            public void onError(Throwable t) {
                LOG.error(t.getMessage(), t);
            }


            @Override
            public void onCompleted() {

            }
        });
    }

    //при перезагрузке
    void askForSnapshot() {
        GetSnapshotDbRequest request = GetSnapshotDbRequest.newBuilder()
                .setUpdateTime(Timestamp.getDefaultInstance())
                .build();
        serviceStub.getSnapshotDb(request, new StreamObserver<GetSnapshotResponse>() {
            @Override
            public void onNext(GetSnapshotResponse value) {
                LOG.info("Part of snapshot from {} ", hostPortAddress);
                store.loadSnapshot(value.getEntityList());
            }

            @Override
            public void onError(Throwable t) {
                LOG.error(t.getMessage(), t);
            }

            @Override
            public void onCompleted() {
                LOG.info("Load snapshot from {} ", hostPortAddress);
            }
        });
    }

    HostPortAddress getHostPortAddress() {
        return hostPortAddress;
    }
}
