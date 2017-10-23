package ru.finam.borsch.cluster.consul;

import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import com.orbitz.consul.*;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.cache.ServiceHealthKey;
import com.orbitz.consul.model.State;
import com.orbitz.consul.model.agent.ImmutableCheck;
import com.orbitz.consul.model.catalog.CatalogService;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.option.ImmutableQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.BorschSettings;
import ru.finam.borsch.HostPortAddress;
import ru.finam.borsch.cluster.Cluster;
import ru.finam.borsch.cluster.MemberListener;
import ru.finam.borsch.partitioner.ServerHolder;
import ru.finam.borsch.partitioner.MemberListenerImpl;
import ru.finam.borsch.rpc.client.BorschClientManager;

import java.util.*;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * Implementation of cluster
 * Created by akhaymovich on 27.09.17.
 */
public class ConsulCluster extends Cluster {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulCluster.class);
    private static final int BORSCH_CHECK_PERIOD = 1000;

    private final AgentClient agentClient;
    private final HealthClient healthClient;
    private final String serviceHolderName;
    private final String serviceHolderId;
    private final String borschIdCheck;
    private final MemberListener memberListener;
    private final ServerHolder serverHolder;
    private final int grpcPort;
    private final ScheduledThreadPoolExecutor scheduledExecutor;
    private static final int HEALTH_LISTENER_TIMEOUT = 12000;


    private final Consumer<Boolean> healthConsumer = new Consumer<Boolean>() {
        @Override
        public void accept(Boolean grpcWorking) {
            if (grpcWorking) {
                registerCheck();
                scheduledExecutor.scheduleWithFixedDelay(() -> {
                    try {
                        agentClient.check(borschIdCheck, State.PASS, "pass");
                    } catch (NotRegisteredException e) {
                        LOG.error(e.getMessage(), e);
                    }
                }, 0, BORSCH_CHECK_PERIOD, TimeUnit.SECONDS);
                createHealthListener();
                synchronizeData();
            }
        }
    };


    public ConsulCluster(BorschClientManager borschClientManager,
                         BorschSettings borschSettings,
                         ScheduledThreadPoolExecutor scheduledExecutor) {
        super(borschClientManager);
        this.scheduledExecutor = scheduledExecutor;

        this.serviceHolderName = borschSettings.getServiceHolderName();
        Consul consul = Consul.builder()
                .withHostAndPort(HostAndPort.fromParts(borschSettings.getConsulHost(),
                        borschSettings.getConsulPort()))
                .withReadTimeoutMillis(60 * 1000L)
                .build();
        this.agentClient = consul.agentClient();
        this.healthClient = consul.healthClient();
        this.serviceHolderId = borschSettings.getServiceHolderId();
        this.borschIdCheck = "borsch_" + serviceHolderId;
        HostPortAddress ownAddress = discoverOwnAddress(consul);
        this.grpcPort = ownAddress.getPort();
        this.serverHolder = new ServerHolder(ownAddress, new ArrayList<>());
        this.memberListener = new MemberListenerImpl(borschClientManager, serverHolder);

    }

    private void createHealthListener() {
        ServiceHealthCache svHealth = ServiceHealthCache.newCache(healthClient, serviceHolderName,
                false, HEALTH_LISTENER_TIMEOUT, ImmutableQueryOptions.builder().build());

        scheduledExecutor.scheduleAtFixedRate(() ->
                svHealth.addListener(newValues -> {
                    for (Map.Entry<ServiceHealthKey, ServiceHealth> servEntry : newValues.entrySet()) {
                        ServiceHealth serviceHealth = servEntry.getValue();
                        ServiceHealthKey healthKey = servEntry.getKey();

                        List<String> tags = serviceHealth.getService().getTags();
                        int port = parseTag(tags);
                        int checkSize = serviceHealth.getChecks().size();
                        long healthyChecks = serviceHealth.getChecks().stream()
                                .filter(healthCheck -> healthCheck.getStatus().equals("passing"))
                                .count();
                        HostPortAddress hostPortAddress = new HostPortAddress(serviceHealth.getNode().getAddress(),
                                port);
                        if (checkSize > healthyChecks) {
                            memberListener.onLeave(hostPortAddress);
                        } else {
                            memberListener.onJoin(hostPortAddress);
                        }
                    }
                }), 0, HEALTH_LISTENER_TIMEOUT, TimeUnit.MILLISECONDS
        );

        try {
            svHealth.start();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private HostPortAddress discoverOwnAddress(Consul consul) {
        CatalogClient catalogClient = consul.catalogClient();
        List<CatalogService> serviceList = catalogClient.getService(serviceHolderName).getResponse();
        CatalogService holderService = serviceList.stream().filter(catalogService ->
                catalogService.getServiceId().equals(serviceHolderId)
        ).findAny().get();
        int port = parseTag(holderService.getServiceTags());
        return new HostPortAddress(holderService.getAddress(), port);
    }

    private void registerCheck() {
        ImmutableCheck options = ImmutableCheck.builder()
                .id(borschIdCheck)
                .serviceId(serviceHolderId)
                .name("borschId")
                .ttl(5 * BORSCH_CHECK_PERIOD + "ms")
                .notes("borschHealth")
                .build();
        agentClient.registerCheck(options);
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                agentClient.deregisterCheck(borschIdCheck)));
    }


    private static int parseTag(List<String> tags) {
        Optional<String> borschTag =
                tags.stream().filter(tag -> tag.contains("borschPort")).findAny();
        if (!borschTag.isPresent()) {
            return -1;
        }
        return Integer.parseInt(borschTag.get().split("=")[1]);
    }

    @Override
    public boolean isMyData(ByteString accountHash) {
        return serverHolder.isMyData(accountHash);
    }

    @Override
    public int quorum() {
        return serverHolder.currentQuorum();
    }

    @Override
    public int numOfMembers() {
        return serverHolder.numOfMembers();
    }

    @Override
    public int grpcPort() {
        return grpcPort;
    }

    @Override
    public Consumer<Boolean> getHealthListener() {
        return healthConsumer;
    }

}
