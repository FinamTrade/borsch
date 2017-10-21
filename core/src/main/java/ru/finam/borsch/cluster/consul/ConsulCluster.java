package ru.finam.borsch.cluster.consul;

import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import com.orbitz.consul.*;
import com.orbitz.consul.cache.ConsulCache;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.cache.ServiceHealthKey;
import com.orbitz.consul.model.State;
import com.orbitz.consul.model.agent.ImmutableCheck;
import com.orbitz.consul.model.catalog.CatalogService;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.option.ImmutableQueryOptions;
import com.orbitz.consul.option.QueryOptions;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * Implementation of cluster
 * Created by akhaymovich on 27.09.17.
 */
public class ConsulCluster extends Cluster {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulCluster.class);
    private static final int BORSCH_CHECK_PERIOD = 10;
    private static final int MASTER_SERVICE_CHECK = 1;
    private static final TimeUnit MASTER_CHECK_TIME = TimeUnit.SECONDS;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AgentClient agentClient;
    private final HealthClient healthClient;
    private final CatalogClient catalogClient;
    private final String serviceHolderName;
    private final String serviceHolderId;
    private final String borschIdCheck;
    private final MemberListener memberListener;
    private final ServerHolder serverHolder;
    private final int grpcPort;


    private final Consumer<Boolean> healthConsumer = new Consumer<Boolean>() {
        @Override
        public void accept(Boolean grpcWorking) {
            if (grpcWorking) {
                registerCheck();
                createHealthListener();
                userHealthService.scheduleWithFixedDelay(() -> {
                    try {
                        agentClient.check(borschIdCheck, State.PASS, "pass");
                    } catch (NotRegisteredException e) {
                        LOG.error(e.getMessage(), e);
                    }
                }, 0, BORSCH_CHECK_PERIOD, TimeUnit.SECONDS);
                healthNotifier();
                synchronizeData();
            }
        }
    };

    private final ScheduledExecutorService userHealthService =
            Executors.newScheduledThreadPool(1);


    public ConsulCluster(BorschClientManager borschClientManager,
                         BorschSettings borschSettings) {
        super(borschClientManager);
        this.serviceHolderName = borschSettings.getServiceHolderName();
        Consul consul = Consul.builder()
                .withHostAndPort(HostAndPort.fromParts(borschSettings.getConsulHost(),
                        borschSettings.getConsulPort()))
                .withReadTimeoutMillis(60 * 100000000)
                .build();
        this.agentClient = consul.agentClient();
        this.healthClient = consul.healthClient();
        this.catalogClient = consul.catalogClient();
        this.serviceHolderId = borschSettings.getServiceHolderId();
        this.borschIdCheck = "borsch_" + serviceHolderId;
        HostPortAddress ownAddress = discoverOwnAddress(consul);
        this.grpcPort = ownAddress.getPort();
        this.serverHolder = new ServerHolder(ownAddress, new ArrayList<>());
        this.memberListener = new MemberListenerImpl(borschClientManager, serverHolder);

    }

    private void createHealthListener() {


        QueryOptions queryOptions = ImmutableQueryOptions.builder()
                .datacenter("dc1")
                .build();
        ServiceHealthCache svHealth = ServiceHealthCache.newCache(healthClient, serviceHolderName,
                false, 12000, queryOptions);

        svHealth.addListener(new ConsulCache.Listener<ServiceHealthKey, ServiceHealth>() {
            @Override
            public void notify(Map<ServiceHealthKey, ServiceHealth> newValues) {
                System.out.println("changes : " + newValues.size());
            }
        });

        try {
            svHealth.start();
        } catch (Exception e) {
            e.printStackTrace();
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
                .ttl(2 * BORSCH_CHECK_PERIOD + "s")
                .notes("borschHealth")
                .build();
        agentClient.registerCheck(options);
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                agentClient.deregisterCheck(borschIdCheck)));
    }

    private void healthNotifier() {
        List<ServiceHealth> healthList = new ArrayList<>();//loadHealthyInstances();
        long time = System.currentTimeMillis();
        healthList.forEach(serviceHealth -> {
            String host = serviceHealth.getService().getAddress();
            if (!serviceHealth.getService().getId().equals(serviceHolderId)) {
                long borschHealthCheck = serviceHealth.getChecks().stream().filter(check -> check.getCheckId().contains("borsch")).count();
                if (borschHealthCheck != 0) {
                    int grpcPort = parseTag(serviceHealth.getService().getTags());
                    HostPortAddress hostPortAddress = new HostPortAddress(host, grpcPort);
                    if (!inetAddressMap.containsKey(hostPortAddress) &&
                            !serviceHealth.getService().getId().equals(serviceHolderId)) {
                        memberListener.onJoin(hostPortAddress);
                    }
                    inetAddressMap.put(hostPortAddress, time);
                }
            }
        });
        Set<HostPortAddress> diedServers = new HashSet<>();
        inetAddressMap.entrySet().stream()
                .filter(healthEntry -> healthEntry.getValue() < time)
                .forEach(entry -> {
                    HostPortAddress diedServer = entry.getKey();
                    memberListener.onLeave(diedServer);
                    diedServers.add(diedServer);
                });
        for (HostPortAddress hostPortAddress : diedServers) {
            inetAddressMap.remove(hostPortAddress);
        }
        scheduler.schedule(this::healthNotifier, MASTER_SERVICE_CHECK, MASTER_CHECK_TIME);
    }

//    private List<ServiceHealth> loadHealthyInstances() {
//        return healthClient.getHealthyServiceInstances(serviceHolderName)
//                .getResponse();
//    }

    private static int parseTag(List<String> tags) {
        Optional<String> borschTag =
                tags.stream().filter(tag -> tag.contains("borschPort")).findAny();
        if (!borschTag.isPresent()) {
            return -1;
        }
        return Integer.parseInt(borschTag.get().split("=")[1]);
    }

    @Override
    public void start() {
        healthNotifier();
        registerCheck();
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
