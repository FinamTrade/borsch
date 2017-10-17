package ru.finam.borsch.cluster.consul;

import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import com.orbitz.consul.*;
import com.orbitz.consul.model.State;
import com.orbitz.consul.model.agent.ImmutableCheck;
import com.orbitz.consul.model.catalog.CatalogService;
import com.orbitz.consul.model.health.ServiceHealth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.BorschSettings;
import ru.finam.borsch.cluster.Cluster;
import ru.finam.borsch.InetAddress;
import ru.finam.borsch.cluster.MemberListener;
import ru.finam.borsch.partitioner.ConsistentHashRing;
import ru.finam.borsch.partitioner.MemberListenerImpl;
import ru.finam.borsch.rpc.client.BorschClientManager;

import java.util.*;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;


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
    private final String serviceHolderName;
    private final String serviceHolderId;
    private final String borschIdCheck;
    private final MemberListener memberListener;
    private final ConsistentHashRing consistentHashRing;
    private final int grpcPort;


    private final Consumer<Boolean> healthConsumer = new Consumer<Boolean>() {
        @Override
        public void accept(Boolean grpcWorking) {
            if (grpcWorking) {
                registerCheck();
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
                .build();
        this.agentClient = consul.agentClient();
        this.healthClient = consul.healthClient();
        this.serviceHolderId = borschSettings.getServiceHolderId();
        this.borschIdCheck = "borsch_" + serviceHolderId;
        InetAddress ownAddress = discoverOwnAddress(consul);
        this.grpcPort = ownAddress.getPort();
        this.consistentHashRing = new ConsistentHashRing(ownAddress, new ArrayList<>());
        this.memberListener = new MemberListenerImpl(borschClientManager, consistentHashRing);
    }

    private InetAddress discoverOwnAddress(Consul consul) {
        CatalogClient catalogClient = consul.catalogClient();
        List<CatalogService> serviceList = catalogClient.getService(serviceHolderName).getResponse();
        CatalogService holderService = serviceList.stream().filter(catalogService ->
                catalogService.getServiceId().equals(serviceHolderId)
        ).findAny().get();
        int port = parseTag(holderService.getServiceTags());
        return new InetAddress(holderService.getAddress(), port);
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
        List<ServiceHealth> healthList = loadHealthyInstances();
        long time = System.currentTimeMillis();
        healthList.forEach(serviceHealth -> {
            String host = serviceHealth.getService().getAddress();
            if (!serviceHealth.getService().getId().equals(serviceHolderId)) {
                long borschHealthCheck = serviceHealth.getChecks().stream().filter(check -> check.getCheckId().contains("borsch")).count();
                if (borschHealthCheck != 0) {
                    int grpcPort = parseTag(serviceHealth.getService().getTags());
                    InetAddress inetAddress = new InetAddress(host, grpcPort);
                    if (!inetAddressMap.containsKey(inetAddress) &&
                            !serviceHealth.getService().getId().equals(serviceHolderId)) {
                        memberListener.onJoin(inetAddress);
                    }
                    inetAddressMap.put(inetAddress, time);
                }
            }
        });
        Set<InetAddress> diedServers = new HashSet<>();
        inetAddressMap.entrySet().stream()
                .filter(healthEntry -> healthEntry.getValue() < time)
                .forEach(entry -> {
                    InetAddress diedServer = entry.getKey();
                    memberListener.inLeave(diedServer);
                    diedServers.add(diedServer);
                });
        for (InetAddress inetAddress : diedServers) {
            inetAddressMap.remove(inetAddress);
        }
        scheduler.schedule(this::healthNotifier, MASTER_SERVICE_CHECK, MASTER_CHECK_TIME);
    }

    private List<ServiceHealth> loadHealthyInstances() {
        return healthClient.getHealthyServiceInstances(serviceHolderName)
                .getResponse();
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
    public void start() {
        healthNotifier();
        registerCheck();
    }

    @Override
    public boolean isMyData(ByteString accountHash) {
        return consistentHashRing.isMyData(accountHash);
    }

    @Override
    public int quorum() {
        return consistentHashRing.currentQuorum();
    }

    @Override
    public int numOfMembers() {
        return consistentHashRing.numOfMembers();
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
