package ru.finam.borsch.launch;

import ru.finam.borsch.BorschSettings;

/**
 * For starting borsch cluster
 * Created by akhaymovich on 18.09.17.
 */
public class StartPoint {

    private final BorschFactory borschDependencyProvider;

    public StartPoint(BorschSettings borschSettings) {
        this.borschDependencyProvider =
                new BorschFactory(borschSettings);
    }

    public void launch() {
        borschDependencyProvider.start();
    }
}
