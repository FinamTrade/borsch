package ru.finam.borsch.rpc.server;

import com.google.protobuf.ByteString;
import finam.protobuf.borsch.*;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.cluster.ClusterInfo;
import ru.finam.borsch.rpc.client.BorschClientManager;
import ru.finam.borsch.time.TimeSource;
import ru.finam.rocksdb.Store;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * Borsch GRPC Api
 * Created by akhaymovich on 15.09.17.
 */
public class BorschServiceApi extends BorschServiceGrpc.BorschServiceImplBase {

    private static final int MAX_REQUEST_SIZE = 200;
    private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    private final TimeSource timeSource;
    private final Store store;
    private final BorschClientManager borschClientManager;
    private final ClusterInfo cluster;

    private static final Logger LOG = LoggerFactory.getLogger(BorschServiceApi.class);


    public BorschServiceApi(ScheduledExecutorService executorService,
                            Store store,
                            ClusterInfo clusterInfo,
                            BorschClientManager borschClientManager) {
        this.store = store;
        this.borschClientManager = borschClientManager;
        this.cluster = clusterInfo;
        this.timeSource = new TimeSource(executorService);
    }


    public void get(finam.protobuf.borsch.GetRequest request,
                    io.grpc.stub.StreamObserver<finam.protobuf.borsch.GetResponse> responseObserver) {
        Optional<KV> result =
                store.get(request.getFamilyName(), request.getKey().toByteArray());
        GetResponse.Builder builder = GetResponse.newBuilder();
        result.ifPresent(builder::setKv);
        responseObserver.onNext(builder.build());
    }

    public void getSnapshot(finam.protobuf.borsch.GetSnapshotFamilyRequest request,
                            io.grpc.stub.StreamObserver<finam.protobuf.borsch.GetSnapshotResponse>
                                    responseObserver) {
        List<KVRecord> kvList = store.getColumnCopy(request.getFamilyName());
        for (int i = 0; i < kvList.size(); i += MAX_REQUEST_SIZE) {
            GetSnapshotResponse.Builder responseBuilder = GetSnapshotResponse.newBuilder();
            for (int j = i; j < kvList.size(); j++) {
                responseBuilder.addEntity(kvList.get(j));
            }
            responseObserver.onNext(responseBuilder.build());
        }
        responseObserver.onCompleted();
    }

    public void getSnapshotFamily(finam.protobuf.borsch.GetSnapshotFamilyRequest request,
                                  io.grpc.stub.StreamObserver<finam.protobuf.borsch.GetSnapshotResponse> responseObserver) {
        List<KVRecord> kvList = store.getColumnCopy(request.getFamilyName());
        sendDbUpdate(kvList, responseObserver);
    }


    public void getSnapshotDb(finam.protobuf.borsch.GetSnapshotDbRequest request,
                              io.grpc.stub.StreamObserver<finam.protobuf.borsch.GetSnapshotResponse> responseObserver) {
        long millisFrom = TimeUnit.SECONDS.toMillis(request.getUpdateTime().getSeconds());
        List<KVRecord> kvList = store.getDbCopy(millisFrom);
        LOG.info("Ask for a snapshot. Having {} records ", kvList.size());
        sendDbUpdate(kvList, responseObserver);
    }

    private static void sendDbUpdate(List<KVRecord> kvList,
                                     StreamObserver<finam.protobuf.borsch.GetSnapshotResponse> responseObserver) {
        for (int i = 0; i < kvList.size(); i += MAX_REQUEST_SIZE) {
            GetSnapshotResponse.Builder responseBuilder = GetSnapshotResponse.newBuilder();
            for (int j = i; j < kvList.size(); j++) {
                responseBuilder.addEntity(kvList.get(j));
            }
            responseObserver.onNext(responseBuilder.build());
        }
        responseObserver.onCompleted();
    }


    public void put(finam.protobuf.borsch.PutRequest request,
                    io.grpc.stub.StreamObserver<finam.protobuf.borsch.PutResponse> responseObserver) {
        ByteString shardPart = request.getKv().getKey().getShardPart();
        if (cluster.isMyData(shardPart)) {
            int quorum;
            store.put(request.getKv());
            switch (request.getMode()) {
                case QUORUM: {
                    quorum = cluster.quorum() - 1;
                    break;
                }
                case ALL: {
                    quorum = cluster.numOfMembers() - 1;
                    break;

                }
                default:
                    try {
                        responseObserver.onNext(PutResponse.newBuilder().setResult(true).build());
                    } catch (Throwable e) {
                        LOG.error(e.getMessage(), e);
                    }
                    return;
            }
            Consumer<Boolean> collectConsumer = new CollectConsumer(timeSource, responseObserver, quorum);
            borschClientManager.putToNeibours(request, collectConsumer);
        } else {
            store.put(request.getKv());
            try {
                responseObserver.onNext(PutResponse
                        .newBuilder()
                        .setResult(true)
                        .build());
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    public void info(finam.protobuf.borsch.ShardInfoRequest request,
                     io.grpc.stub.StreamObserver<finam.protobuf.borsch.ShardInfoResponse> responseObserver) {
        //TODO с консулом в текущей версии не нужно
    }

    private static class CollectConsumer implements Consumer<Boolean> {
        private final int succeedResp;
        private final StreamObserver<PutResponse> responseObserver;

        private boolean working = true;
        private int success;

        CollectConsumer(TimeSource timeSource,
                        StreamObserver<PutResponse> responseObserver,
                        int succeedResp) {
            this.succeedResp = succeedResp;
            this.responseObserver = responseObserver;
            long currentTime = System.currentTimeMillis() + TIME_UNIT.toMillis(1000);
            timeSource.when(currentTime).subscribe(time -> {
                if (succeedResp >= success) {
                    return;
                }
                responseObserver.onNext(PutResponse.newBuilder().setResult(false).build());
                working = false;
                LOG.info("Cancelled by timeout success operations {} target {} ", success, succeedResp);
            });
        }

        @Override
        public void accept(Boolean result) {
            if (!working) {
                return;
            }
            if (result) {
                success++;
            }
            if (succeedResp >= success) {
                responseObserver.onNext(PutResponse.newBuilder().setResult(true).build());
            }
        }
    }

}
