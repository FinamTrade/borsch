package ru.finam.borsch;

/**
 * Project Settings
 * Created by akhaymovich on 27.09.17.
 */
public class BorschSettings {
    private final String consulHost;
    private final int consulPort;
    private final String serviceHolderId;
    private final String serviceHolderName;
    private final String pathToDb;
    private final String pathToTimeFile;
    private final int hashRingPort;

    public BorschSettings(String consulHost,
                          int consulPort,
                          String serviceHolderId,
                          String serviceHolderName,
                          String pathToDb,
                          String pathToTimeFile,
                          int hashRingPort) {
        this.consulHost = consulHost;
        this.consulPort = consulPort;
        this.serviceHolderId = serviceHolderId;
        this.serviceHolderName = serviceHolderName;
        this.pathToDb = pathToDb;
        this.pathToTimeFile = pathToTimeFile;
        this.hashRingPort = hashRingPort;
    }

    public String getConsulHost() {
        return consulHost;
    }

    public int getConsulPort() {
        return consulPort;
    }

    public String getServiceHolderId() {
        return serviceHolderId;
    }

    public String getServiceHolderName() {
        return serviceHolderName;
    }

    public String getPathToDb() {
        return pathToDb;
    }

    public String getPathToTimeFile() {
        return pathToTimeFile;
    }

    public int getHashRingPort() {
        return hashRingPort;
    }
}
