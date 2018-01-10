package ru.finam.borsch.time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * Time source
 * Created by akhaymovich on 01.12.16.
 */
public class TimeSource {

    private final static Logger LOG = LoggerFactory.getLogger(TimeSource.class);

    private static final long PERIOD = 100L;

    private final Object sinkLock = new Object();
    private final List<FluxSink<Long>> sinkList = new ArrayList<>();

    private final Consumer<FluxSink<Long>> timeConsumer = timeFluxSink -> {
        synchronized (sinkLock) {
            sinkList.add(timeFluxSink);
        }
        timeFluxSink.onDispose(() -> {
            synchronized (sinkLock) {
                sinkList.remove(timeFluxSink);
            }
        });
    };

    private final Flux<Long> fluxTime = Flux.create(timeConsumer);
    private final Scheduler scheduler;

    public TimeSource(ExecutorService executorService) {
        scheduler = Schedulers.fromExecutorService(executorService);
        scheduler.schedulePeriodically(() -> {
            long currentTime = System.currentTimeMillis();
            synchronized (sinkLock) {
                sinkList.forEach(sink -> sink.next(currentTime));
            }
        }, 0, PERIOD, TimeUnit.MILLISECONDS);
        scheduler.start();
    }

    public Mono<Long> when(long time) {
        return fluxTime.filter(t -> t >= time).next().doOnError(throwable -> {
            LOG.error("ERROR IN TIME SOURCE  for time {}", new Date(time));
            LOG.error(throwable.getMessage(), throwable);
        });
    }
}
