package ru.finam.borsch.cluster.consul;

import com.orbitz.consul.model.State;
import io.grpc.ManagedChannel;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.okhttp.OkHttpChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.HostPortAddress;

import java.util.concurrent.TimeUnit;

/**
 * Use
 * Created by akhaymovich on 23.10.17.
 */
public class HealthCheckClient {

    private static final Logger LOG = LoggerFactory.getLogger(HealthCheckClient.class);

    private final HealthGrpc.HealthBlockingStub healthStub;

    public HealthCheckClient(HostPortAddress hostPortAddress) {
        ManagedChannel managedChannel = OkHttpChannelBuilder.forAddress(hostPortAddress.getHost(), hostPortAddress.getPort())
                .usePlaintext(true)
                .idleTimeout(1, TimeUnit.MINUTES)
                .build();
        this.healthStub = HealthGrpc.newBlockingStub(managedChannel);
    }

    public State startPing() {
        HealthCheckRequest healthCheckRequest = HealthCheckRequest
                .newBuilder()
                .setService("borsch")
                .build();
        HealthCheckResponse response = healthStub.check(healthCheckRequest);
        switch (response.getStatus()) {
            case SERVING: {
                return State.PASS;
            }
            case UNKNOWN: {
                return State.UNKNOWN;
            }
            case NOT_SERVING:
            default: {
                return State.FAIL;
            }
        }
    }

}
