package ru.finam.borsch.cluster;


import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.BorschSettings;
import ru.finam.borsch.HostPortAddress;
import ru.finam.borsch.rpc.client.BorschClientManager;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Created by akhaymovich on 06.09.17.
 */
public abstract class Cluster implements ClusterInfo {

    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);
    private final String timeFileName;

    protected final Map<HostPortAddress, Long> inetAddressMap = new HashMap<>();
    private final BorschClientManager borschClientManager;
    protected final ScheduledThreadPoolExecutor scheduledExecutor;


    public Cluster(BorschClientManager borschClientManager,
                   ScheduledThreadPoolExecutor scheduledExecutor,
                   BorschSettings borschSettings) {
        this.borschClientManager = borschClientManager;
        this.scheduledExecutor = scheduledExecutor;
        this.timeFileName = borschSettings.getPathToTimeFile() + "/"
                + borschSettings.getServiceHolderId();
    }

    protected void synchronizeData() {
        LOG.info("synchronizing with {} servers", inetAddressMap.size());
        borschClientManager.onClusterStart(getLastWorkTime());
        scheduledExecutor.scheduleWithFixedDelay(() -> writeWorkTimeToFile(),
                1L, 1L, TimeUnit.MINUTES);
    }

    public abstract Consumer<Boolean> getHealthListener();


    private void writeWorkTimeToFile() {
        File f = new File(timeFileName);
        try {
            f.createNewFile();
            byte[] newTimeBytes = String.valueOf(System.currentTimeMillis()).getBytes();
            Files.write(newTimeBytes, f);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private long getLastWorkTime() {
        File f = new File(timeFileName);
        if (!f.exists()) {
            return 0;
        }
        try {
            List<String> fileContent = Files.readLines(f, Charset.defaultCharset());
            if (fileContent.size() > 1) {
                throw new RuntimeException("Invalid save time");
            }
            return Long.parseLong(fileContent.get(0)) - TimeUnit.MINUTES.toMillis(1);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return 0;
        }
    }
}
