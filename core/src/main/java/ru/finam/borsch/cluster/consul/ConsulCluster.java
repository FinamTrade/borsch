package ru.finam.borsch.cluster.consul;

import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import com.orbitz.consul.*;
import com.orbitz.consul.async.ConsulResponseCallback;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.State;
import com.orbitz.consul.model.agent.ImmutableCheck;
import com.orbitz.consul.model.catalog.CatalogService;
import com.orbitz.consul.model.health.HealthCheck;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.option.QueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.BorschSettings;
import ru.finam.borsch.HostPortAddress;
import ru.finam.borsch.cluster.Cluster;
import ru.finam.borsch.cluster.MemberListener;
import ru.finam.borsch.partitioner.ServerDistributionHolder;
import ru.finam.borsch.partitioner.MemberListenerImpl;
import ru.finam.borsch.rpc.client.BorschClientManager;

import java.math.BigInteger;
import java.util.*;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;


/**
 * Implementation of cluster
 * Created by akhaymovich on 27.09.17.
 */
public class ConsulCluster extends Cluster {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulCluster.class);
    private static final int BORSCH_CHECK_PERIOD = 10000;

    private final AgentClient agentClient;
    private final HealthClient healthClient;
    private final String serviceHolderName;
    private final String serviceHolderId;
    private final String borschIdCheck;
    private final MemberListener memberListener;
    private final ServerDistributionHolder serverDistributionHolder;
    private final ScheduledThreadPoolExecutor scheduledExecutor;
    private final int grpcPort;
    private final AtomicReference<BigInteger> index = new AtomicReference<>(BigInteger.ZERO);
    private final HostPortAddress ownAddress;



    private final ConsulResponseCallback<List<ServiceHealth>> healthCallback =
            new ConsulResponseCallback<List<ServiceHealth>>() {

                @Override
                public void onComplete(ConsulResponse<List<ServiceHealth>> consulResponse) {
                    healthNotifier(consulResponse.getResponse());
                    if (index.get().equals(BigInteger.ZERO)){
                        synchronizeData();
                    }
                    index.set(consulResponse.getIndex());
                    loadHealthyInstances(index);
                }

                @Override
                public void onFailure(Throwable throwable) {
                    LOG.debug("Timeout on blocking query ", throwable);
                    loadHealthyInstances(index);
                }
            };

    private final Consumer<Boolean> healthConsumer = new Consumer<Boolean>() {
        @Override
        public void accept(Boolean grpcWorking) {
            registerCheck();
            if (grpcWorking) {
                scheduledExecutor.scheduleWithFixedDelay(() -> {
                    try {
                        agentClient.check(borschIdCheck, State.PASS, "pass");
                    } catch (NotRegisteredException e) {
                        LOG.error(e.getMessage(), e);
                    }
                }, 0, BORSCH_CHECK_PERIOD, TimeUnit.MILLISECONDS);
                loadHealthyInstances(index);
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
        this.ownAddress = ownAddress;
        this.serverDistributionHolder = new ServerDistributionHolder(ownAddress, new ArrayList<>());
        this.memberListener = new MemberListenerImpl(borschClientManager, serverDistributionHolder);
    }

    private void loadHealthyInstances(AtomicReference<BigInteger> index) {
        healthClient.getHealthyServiceInstances(
                serviceHolderName,
                QueryOptions.blockMinutes(5, index.get()).build(),
                healthCallback);
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
                .ttl(10 * BORSCH_CHECK_PERIOD + "ms")
                .notes("borschHealth")
                .build();
        agentClient.registerCheck(options);
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                agentClient.deregisterCheck(borschIdCheck)));
    }

    private void healthNotifier(List<ServiceHealth> healthList) {
        long time = System.currentTimeMillis();
        healthList.forEach(serviceHealth -> {
            String host = serviceHealth.getService().getAddress();
            if (!serviceHealth.getService().getId().equals(serviceHolderId)) {
                long borschHealthCheck = serviceHealth.getChecks().stream().filter(check -> check.getCheckId().contains("borsch")).count();
                if (borschHealthCheck != 0) {
                    int grpcPort = parseTag(serviceHealth.getService().getTags());
                    HostPortAddress inetAddress = new HostPortAddress(host, grpcPort);
                    if (!inetAddressMap.containsKey(inetAddress) &&
                            !serviceHealth.getService().getId().equals(serviceHolderId)                           ) {
                        memberListener.onJoin(inetAddress);
                    }
                    inetAddressMap.put(inetAddress, time);
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
        for (HostPortAddress inetAddress : diedServers) {
            inetAddressMap.remove(inetAddress);
        }
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
