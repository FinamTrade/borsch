package ru.finam.borsch.rpc.client;

import com.google.protobuf.Timestamp;
import finam.protobuf.borsch.*;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
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
        ManagedChannel managedChannel = NettyChannelBuilder.forAddress(hostPortAddress.getHost(), hostPortAddress.getGrpcBorschPort())
                .usePlaintext(true)
                .idleTimeout(1, TimeUnit.MINUTES)
                .build();
        serviceStub = BorschServiceGrpc.newStub(managedChannel).withWaitForReady();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            managedChannel.shutdownNow();
        }));
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
    void askForSnapshot(Consumer<Void> onReady, long updatedSinceMillis) {
        GetSnapshotDbRequest request = GetSnapshotDbRequest.newBuilder()
                .setUpdateTime(Timestamp.newBuilder().setSeconds(TimeUnit.MILLISECONDS.toSeconds(updatedSinceMillis)).build())
                .build();
        serviceStub.getFullSnapshotDb(request, new StreamObserver<GetSnapshotResponse>() {

            @Override
            public void onNext(GetSnapshotResponse value) {
                store.loadSnapshot(value.getEntityList());
            }

            @Override
            public void onError(Throwable t) {
                LOG.error(t.getMessage(), t);
            }

            @Override
            public void onCompleted() {
                LOG.info("Load snapshot from {} ", hostPortAddress);
                onReady.accept(null);
            }
        });
    }

    HostPortAddress getHostPortAddress() {
        return hostPortAddress;
    }
}
