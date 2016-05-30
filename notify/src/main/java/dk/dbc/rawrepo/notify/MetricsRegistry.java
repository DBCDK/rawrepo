/*
 * dbc-rawrepo-notify
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-notify.
 *
 * dbc-rawrepo-notify is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-notify is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-notify.  If not, see <http://www.gnu.org/licenses/>.
 */

package dk.dbc.rawrepo.notify;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.Singleton;

/**
 *
 * @author thp
 */
@Singleton
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

    public MetricRegistry getRegistry() {
        return metrics;
    }
}
