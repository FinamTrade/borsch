package ru.finam.borsch.rpc.client;

import java.util.function.Consumer;


/**
 * Created by akhaymovich on 23.10.17.
 */
public class HealthCheckClient {

    private Consumer<Boolean> healthChecker;

    public HealthCheckClient(Consumer<Boolean> subscription) {

    }
}
