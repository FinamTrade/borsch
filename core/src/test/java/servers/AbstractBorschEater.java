package servers;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import ru.finam.borsch.BorschSettings;
import ru.finam.borsch.HostPortAddress;
import ru.finam.borsch.launch.StartPoint;

import java.util.List;

/**
 * Created by akhaymovich on 08.10.17.
 */
public abstract class AbstractBorschEater {

    private final AgentClient agentClient;
    private final List<String> borschTags;
    private final BorschSettings borschSettings;
    private final HostPortAddress grpcBorschAddress;
    private static final String SERVICE_NAME = "dev-ftcore-borsch-eater";
    private final SimpleGrpcClient grpcClient;
    private final int shard;

    private static final String consulHost = "localhost";
    private static final int consulPort = 8500;


    public AbstractBorschEater(HostPortAddress grpcBorschAddress,
                                                           int shard) {
        Consul consul = Consul.builder()
                .withHostAndPort(HostAndPort.fromParts(consulHost,
                        consulPort))
                .build();
        this.agentClient = consul.agentClient();
        this.borschTags = ImmutableList.of(
                "borschHost=" + grpcBorschAddress.getHost(),
                "borschPort=" + grpcBorschAddress.getPort());
        this.borschSettings = new BorschSettings(consulHost,
                consulPort, getServiceId(),
                SERVICE_NAME,
                "/var/lib/borsch/db/" + getServiceId());
        this.grpcBorschAddress = grpcBorschAddress;
        this.grpcClient = new SimpleGrpcClient(grpcBorschAddress);
        this.shard = shard;
    }

    public void registerService() {
        String address = agentClient.getAgent().getMember().getAddress();
        ImmutableRegistration registration = ImmutableRegistration.builder()
                .name(SERVICE_NAME)
                .id(getServiceId())
                .address(address)
                .tags(borschTags)
                .build();
        agentClient.register(registration);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            agentClient.deregister(getServiceId());
        }));
    }

    public void launchBorsch() {
        StartPoint startPoint = new StartPoint(borschSettings);
        startPoint.launch();
        new BorschDataThread(grpcClient, shard, getServiceId());
    }

    protected abstract String getServiceId();
}
