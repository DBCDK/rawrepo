/*
 * dbc-rawrepo-content-service
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-content-service.
 *
 * dbc-rawrepo-content-service is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-content-service is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-content-service.  If not, see <http://www.gnu.org/licenses/>.
 */
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
 * @author DBC <dbc.dk>
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
