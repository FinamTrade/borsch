package servers;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.BorschSettings;
import ru.finam.borsch.HostPortAddress;
import ru.finam.borsch.launch.BorschFactory;

import java.util.List;

public abstract class AbstractBorschEater {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBorschEater.class);

    private final AgentClient agentClient;
    private final List<String> borschTags;
    private final BorschSettings borschSettings;

    private static final String SERVICE_NAME = "dev-ftcore-borsch-eater";
    private final SimpleGrpcClient grpcClient;
    private final int shard;

    private static final String consulHost = "localhost";
    private static final int consulPort = 8500;


    AbstractBorschEater(HostPortAddress grpcBorschAddress,
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
                "/home/akhaymovich/temp/txalerts",
                "/home/akhaymovich/temp/txalerts/"
        );
        this.grpcClient = new SimpleGrpcClient(grpcBorschAddress);
        this.shard = shard;
    }

    void registerService() {
        String address = agentClient.getAgent().getMember().getAddress();
        ImmutableRegistration registration = ImmutableRegistration.builder()
                .name(SERVICE_NAME)
                .id(getServiceId())
                .address(address)
                .tags(borschTags)
                .build();
        agentClient.register(registration);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> agentClient.deregister(getServiceId())));
    }

    void launchBorsch() {
        BorschFactory borschFactory = new BorschFactory(() -> {
            LOG.info("Stop data");
        },
                () -> {
                    LOG.info("Start data");
                },
                borschSettings);
        borschFactory.startBorsch();
        borschFactory.isMyEntity("BUCHMCU25225");
        new BorschDataThread(grpcClient, shard, getServiceId());
    }

    protected abstract String getServiceId();
}
