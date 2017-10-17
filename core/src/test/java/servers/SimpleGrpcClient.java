package servers;

import finam.protobuf.borsch.*;
import io.grpc.ManagedChannel;
import io.grpc.okhttp.OkHttpChannelBuilder;
import io.grpc.stub.StreamObserver;
import ru.finam.borsch.InetAddress;
import java.util.concurrent.TimeUnit;

/**
 * Created by akhaymovich on 08.10.17.
 */
public class SimpleGrpcClient {

    private ManagedChannel managedChannel;
    private BorschServiceGrpc.BorschServiceStub serviceStub;

    public SimpleGrpcClient(InetAddress inetAddress) {
        managedChannel = OkHttpChannelBuilder.forAddress(inetAddress.getHost(), inetAddress.getPort())
                .usePlaintext(true)
                .idleTimeout(1, TimeUnit.MINUTES)
                .build();
        serviceStub = BorschServiceGrpc.newStub(managedChannel);
    }


    public void put(PutRequest putRequest) {
        serviceStub.put(putRequest, new StreamObserver<PutResponse>() {
            @Override
            public void onNext(PutResponse value) {
    //            System.out.println(value);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onCompleted() {

            }
        });
    }

    //при перезагрузке
    public void askForSnapshot() {
        GetSnapshotDbRequest request = GetSnapshotDbRequest.newBuilder().build();
        serviceStub.getSnapshotDb(request, new StreamObserver<GetSnapshotResponse>() {
            @Override
            public void onNext(GetSnapshotResponse value) {

            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        });
    }

}
