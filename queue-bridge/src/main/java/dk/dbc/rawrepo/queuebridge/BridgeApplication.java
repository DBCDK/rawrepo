/*
 * Copyright (C) 2017 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-queue-bridge
 *
 * dbc-rawrepo-queue-bridge is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-queue-bridge is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.queuebridge;

import com.codahale.metrics.MetricRegistry;
import dk.dbc.dropwizard.DbcApplication;
import io.dropwizard.setup.Environment;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class BridgeApplication extends DbcApplication<BridgeConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BridgeApplication.class);

    public static void main(String[] args) {
        try {
            new BridgeApplication().run(args);
        } catch (Exception exception) {
            log.error("Exception: " + exception.getMessage());
            log.debug("Exception:", exception);
        }
    }

    @Override
    public String getName() {
        return "rawrepo-queue-bridge";
    }

    @Override
    public void run(BridgeConfiguration cfg, Environment env) throws Exception {
        MetricRegistry metrics = env.metrics();

        DataSource dataSource = cfg.getDataSourceFactory().build(metrics, getName());

        DbToMqDaemon dbToMqDaemon = DbToMqDaemon.start(metrics, cfg.getWorkerConfiguration(), dataSource);
        MqToDbDaemon mqToDbDaemon = MqToDbDaemon.start(metrics, cfg.getWorkerConfiguration(), dataSource);

        env.healthChecks().register("dbToMq", dbToMqDaemon);
        env.healthChecks().register("mqToDb", mqToDbDaemon);
    }

}
