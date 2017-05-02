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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.sun.messaging.ConnectionConfiguration;
import com.sun.messaging.ConnectionFactory;
import dk.dbc.dropwizard.DaemonMaster;
import dk.dbc.rawrepo.QueueJob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.Session;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class DbToMqDaemon extends HealthCheck {

    private static final Logger log = LoggerFactory.getLogger(DbToMqDaemon.class);

    private final MetricRegistry metrics;
    private final WorkerConfiguration cfg;
    private final DataSource dataSource;
    private final Counter processedCounter;
    private final Map<String, String> queueRules;
    private final String sql;
    private DaemonMaster master;

    static DbToMqDaemon start(MetricRegistry metrics, WorkerConfiguration cfg, DataSource dataSource) {
        DbToMqDaemon daemon = new DbToMqDaemon(metrics, cfg, dataSource);
        Integer threads = cfg.getDbToMqThreads();
        if (daemon.queueRules.isEmpty()) {
            log.warn("No queueRules defined");
            threads = 0;
        }
        DaemonMaster master = DaemonMaster.start(threads, daemon::makeDbToMqWorker);
        daemon.setMaster(master);
        return daemon;
    }

    private DbToMqDaemon(MetricRegistry metrics, WorkerConfiguration cfg, DataSource dataSource) {
        this.metrics = metrics;
        this.cfg = cfg;
        this.dataSource = dataSource;
        String prefix = getClass().getCanonicalName() + ".";
        this.processedCounter = metrics.counter(prefix + "processed");
        this.queueRules = queueRules(cfg, dataSource);
        this.sql = "SELECT bibliographicrecordid, agencyid, worker, queued, ctid FROM queue WHERE worker IN('" +
                   queueRules.keySet().stream().map(s -> s.replace("'", "''")).collect(Collectors.joining("', '")) +
                   "') ORDER BY queued FOR UPDATE";
        log.debug("sql = " + sql);
    }

    static Map<String, String> queueRules(WorkerConfiguration cfg, DataSource dataSource) {
        Set<String> queues = Arrays.stream(cfg.getDbToMqQueues().split(","))
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
        HashMap<String, String> map = new HashMap<>();
        try (Connection connection = dataSource.getConnection() ;
             PreparedStatement stmt = connection.prepareStatement("SELECT worker, queuename FROM messagequeuerules") ;
             ResultSet resultSet = stmt.executeQuery()) {
            while (resultSet.next()) {
                String from = resultSet.getString(1);
                String to = resultSet.getString(2);
                if (queues.contains(from)) {
                    map.put(from, to);
                }
            }
        } catch (SQLException ex) {
            log.error("Error Building queueRules: " + ex.getMessage());
            log.debug("Error Building queueRules:", ex);
        }
        return map;
    }

    private void setMaster(DaemonMaster master) {
        this.master = master;
    }

    @Override
    protected Result check() throws Exception {
        int wanted = master.getThreadCount();
        int running = master.getRunningThreadCount();
        if (running != wanted) {
            return Result.unhealthy("thread count mismatch {} != {}", wanted, running);
        }
        return Result.healthy();
    }

    private DbToMqWorker makeDbToMqWorker() {
        return new DbToMqWorker();
    }

    JMSContext createJMSContext() throws JMSException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setProperty(ConnectionConfiguration.imqAddressList, cfg.getQueueServer());
        return connectionFactory.createContext(Session.AUTO_ACKNOWLEDGE);
    }

    private class DbToMqWorker implements Runnable {

        HashMap<String, Destination> targets;

        private DbToMqWorker() {
            this.targets = new HashMap<>();
        }

        @Override
        public void run() {
            try {
                try (JMSContext jmsContext = createJMSContext() ;
                     Connection connection = dataSource.getConnection() ;
                     Statement stmt = connection.createStatement() ;
                     PreparedStatement delete = connection.prepareStatement("DELETE FROM queue WHERE ctid = ?")) {
                    try (Statement tz = connection.createStatement()) {
                        tz.executeUpdate("SET TIME ZONE 'UTC'");
                    }

                    connection.setAutoCommit(false);

                    JMSProducer producer = jmsContext.createProducer();
                    for (;;) {

                        try (ResultSet resultSet = stmt.executeQuery(sql)) {
                            if (resultSet.next()) {
                                do {
                                    processedCounter.inc();
                                    String bibliographicRecordId = resultSet.getString(1);
                                    int agencyId = resultSet.getInt(2);
                                    String worker = resultSet.getString(3);
                                    Timestamp queued = resultSet.getTimestamp(4);
                                    Object ctid = resultSet.getObject(5);

                                    delete.setObject(1, ctid);
                                    delete.executeUpdate();

                                    String target = queueRules.get(worker);

                                    QueueJob job = new QueueJob(bibliographicRecordId, agencyId);
                                    if (target == null) {
                                        log.error("Cannot find target for: " + worker + " job is: " + job);
                                    } else {
                                        try {
                                            Destination dest = targets.computeIfAbsent(target, jmsContext::createQueue);
                                            producer.send(dest, job.jmsMessage(jmsContext));
                                        } catch (RuntimeException ex) {
                                            log.error("Exception: " + ex.getMessage());
                                            log.debug("Exception:", ex);
                                            log.warn("Failed Moved " + agencyId + ":" + bibliographicRecordId + " from: " + worker + " to " + target);
                                            connection.rollback();
                                            throw ex;
                                        }
                                        log.info("Moved " + agencyId + ":" + bibliographicRecordId + " from: " + worker + " to " + target);
                                    }
                                } while (resultSet.next());
                                connection.commit();
                                connection.setAutoCommit(false);
                            } else {
                                Thread.sleep(1000L * cfg.getPollInterval());
                            }
                        }
                    }

                }

            } catch (Exception ex) {
                log.error("Error in worker thread: " + ex.getMessage());
                log.debug("Error in worker thread:", ex);
            }
        }
    }

}
