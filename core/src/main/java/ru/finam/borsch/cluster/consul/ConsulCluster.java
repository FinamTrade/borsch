package ru.finam.borsch.cluster.consul;

import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import com.orbitz.consul.*;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.cache.ServiceHealthKey;
import com.orbitz.consul.model.State;
import com.orbitz.consul.model.agent.ImmutableCheck;
import com.orbitz.consul.model.catalog.CatalogService;
import com.orbitz.consul.model.health.HealthCheck;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.option.ImmutableQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.BorschSettings;
import ru.finam.borsch.HostPortAddress;
import ru.finam.borsch.cluster.Cluster;
import ru.finam.borsch.cluster.MemberListener;
import ru.finam.borsch.partitioner.ServerDistributionHolder;
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
    private static final int BORSCH_CHECK_PERIOD = 10000;
    private static final int HEALTH_LISTENER_TIMEOUT = 1200;

    private final AgentClient agentClient;
    private final HealthClient healthClient;
    private final String serviceHolderName;
    private final String serviceHolderId;
    private final String borschIdCheck;
    private final MemberListener memberListener;
    private final ServerDistributionHolder serverDistributionHolder;
    private final ScheduledThreadPoolExecutor scheduledExecutor;
    private final int grpcPort;

    private final HealthCheckClient healthCheckClient;

    private final Consumer<Boolean> healthConsumer = new Consumer<Boolean>() {
        @Override
        public void accept(Boolean grpcWorking) {
            registerCheck();
            if (grpcWorking) {
                scheduledExecutor.scheduleWithFixedDelay(() -> {
                    try {
                        //   State state = healthCheckClient.startPing();
                        //   System.out.println(state);
                        agentClient.check(borschIdCheck, State.PASS, "pass");
                    } catch (NotRegisteredException e) {
                        LOG.error(e.getMessage(), e);
                    }
                }, 0, BORSCH_CHECK_PERIOD/10, TimeUnit.MILLISECONDS);
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
        this.serverDistributionHolder = new ServerDistributionHolder(ownAddress, new ArrayList<>());
        this.memberListener = new MemberListenerImpl(borschClientManager, serverDistributionHolder);
        this.healthCheckClient = new HealthCheckClient(ownAddress);
    }

    private void createHealthListener() {
        ServiceHealthCache svHealth = ServiceHealthCache.newCache(healthClient, serviceHolderName,
                false, 0, ImmutableQueryOptions.builder().build());
        svHealth.addListener(newValues -> {
                    for (Map.Entry<ServiceHealthKey, ServiceHealth> servEntry : newValues.entrySet()) {
                        ServiceHealth serviceHealth = servEntry.getValue();

                        List<String> tags = serviceHealth.getService().getTags();
                        int port = parseTag(tags);
                        List<HealthCheck> healthChecks = serviceHealth.getChecks();
                        int checkSize = healthChecks.size();
                        long healthyChecks = healthChecks.stream()
                                .filter(healthCheck -> healthCheck.getStatus().equals("passing"))
                                .count();
                        HostPortAddress hostPortAddress = new HostPortAddress(serviceHealth.getNode().getAddress(),
                                port);
                        if (checkSize > healthyChecks) {
                            memberListener.onLeave(hostPortAddress);
                        } else if (isHaveBorschCheck(healthChecks)) {
                            memberListener.onJoin(hostPortAddress);
                        }

                    }
                }
        );

        try {
            svHealth.start();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private boolean isHaveBorschCheck(List<HealthCheck> checkList) {
        return checkList.stream()
                .filter(check -> check.getCheckId().equals(borschIdCheck))
                .count() > 0;
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
        return serverDistributionHolder.isMyData(accountHash);
    }

    @Override
    public int numOfMembers() {
        return serverDistributionHolder.numOfMembers();
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
