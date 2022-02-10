package dk.dbc.rawrepo.content.service;

import com.codahale.metrics.Counter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@Singleton
@Startup
public class MetricsRegistry {

    private final MetricRegistry metrics = new MetricRegistry();

    private JmxReporter reporter;

    @PostConstruct
    public void create() {
        reporter = JmxReporter.forRegistry(metrics).build();
        reporter.start();
    }

    @PreDestroy
    public void destroy() {
        if (reporter != null) {
            reporter.stop();
        }
    }

    @Produces
    public Timer makeTimer(InjectionPoint ip) {
        Class<?> clazz = ip.getMember().getDeclaringClass();
        String name = ip.getMember().getName();
        if (name.endsWith("Timer")) {
            name = name.substring(0, name.length() - "Timer".length());
        }
        return metrics.timer(MetricRegistry.name(clazz, name));
    }

    @Produces
    public Counter makeCounter(InjectionPoint ip) {
        Class<?> clazz = ip.getMember().getDeclaringClass();
        String name = ip.getMember().getName();
        if (name.endsWith("Counter")) {
            name = name.substring(0, name.length() - "Counter".length());
        }
        return metrics.counter(MetricRegistry.name(clazz, name));
    }

}
