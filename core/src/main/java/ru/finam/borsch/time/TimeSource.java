package ru.finam.borsch.time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * Time source
 * Created by akhaymovich on 01.12.16.
 */
public class TimeSource {

    private final static Logger LOG = LoggerFactory.getLogger(TimeSource.class);
    private final static long PERIOD = 100L;

    private FluxSink<Long> timeFluxSink;
    private final Consumer<FluxSink<Long>> timeConsumer = timeFluxSink -> {
        this.timeFluxSink = timeFluxSink;
    };

    private final Flux<Long> fluxTime = Flux.create(timeConsumer).share();
    private final Scheduler scheduler = Schedulers.newSingle("timer source");


    public TimeSource() {
        scheduler.schedulePeriodically(() -> {
            long currentTime = System.currentTimeMillis();
            timeFluxSink.next(currentTime);
        }, 0, PERIOD, TimeUnit.MILLISECONDS);
        fluxTime.subscribe();
        scheduler.start();
    }

    public Mono<Long> when(long time) {
        return fluxTime.filter(t -> t >= time).next().doOnError(throwable -> {
            LOG.error("ERROR IN TIME SOURCE  for time {}", new Date(time));
            LOG.error(throwable.getMessage(), throwable);
        });
    }
}
