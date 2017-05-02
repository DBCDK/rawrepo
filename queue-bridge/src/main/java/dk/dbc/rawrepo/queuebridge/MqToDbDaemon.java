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
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class MqToDbDaemon extends HealthCheck {

    private static final Logger log = LoggerFactory.getLogger(MqToDbDaemon.class);

    private final MetricRegistry metrics;
    private final WorkerConfiguration cfg;
    private final DataSource dataSource;
    private final Counter processedCounter;
    private final Map<String, String> queueRules;
    private DaemonMaster master;

    static MqToDbDaemon start(MetricRegistry metrics, WorkerConfiguration cfg, DataSource dataSource) {
        MqToDbDaemon daemon = new MqToDbDaemon(metrics, cfg, dataSource);
        Integer threads = cfg.getMqToDbThreads();
        if (daemon.queueRules.isEmpty()) {
            log.warn("No queueRules defined");
            threads = 0;
        }
        DaemonMaster master = DaemonMaster.start(threads, daemon::makeMqToDbWorker);
        daemon.setMaster(master);
        return daemon;
    }

    private MqToDbDaemon(MetricRegistry metrics, WorkerConfiguration cfg, DataSource dataSource) {
        this.metrics = metrics;
        this.cfg = cfg;
        this.dataSource = dataSource;
        String prefix = getClass().getCanonicalName() + ".";
        this.processedCounter = metrics.counter(prefix + "processed");
        this.queueRules = queueRules(cfg, dataSource);
    }

    static Map<String, String> queueRules(WorkerConfiguration cfg, DataSource dataSource) {
        Set<String> queues = Arrays.stream(cfg.getMqToDbQueues().split(","))
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
        HashMap<String, String> map = new HashMap<>();
        try (Connection connection = dataSource.getConnection() ;
             PreparedStatement stmt = connection.prepareStatement("SELECT queuename, worker FROM messagequeuerules") ;
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

    private MqToDbWorker makeMqToDbWorker() {
        return new MqToDbWorker();
    }

    JMSContext createJMSContext() throws JMSException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setProperty(ConnectionConfiguration.imqAddressList, cfg.getQueueServer());
        return connectionFactory.createContext(Session.AUTO_ACKNOWLEDGE);
    }

    private class MqToDbWorker implements Runnable {

        @Override
        public void run() {

            try {
                HashMap<String, JMSConsumer> consumers = new HashMap<>();
                try (JMSContext jmsContext = createJMSContext() ;
                     Connection connection = dataSource.getConnection() ;
                     PreparedStatement stmt = connection.prepareStatement("INSERT INTO QUEUE (bibliographicrecordid, agencyid, worker, queued) VALUES(?, ?, ?, ?)")) {
                    try (Statement tz = connection.createStatement()) {
                        tz.executeUpdate("SET TIME ZONE 'UTC'");
                    }
                    for (String key : queueRules.keySet()) {
                        consumers.put(key, jmsContext.createConsumer(jmsContext.createQueue(key)));
                    }

                    connection.setAutoCommit(false);
                    jmsContext.start();
                    for (;;) {
                        boolean gotMessage = false;

                        for (Map.Entry<String, JMSConsumer> entry : consumers.entrySet()) {
                            Message message;
                            while (( message = entry.getValue().receiveNoWait() ) != null) {
                                gotMessage = true;
                                QueueJob job = QueueJob.fromMessage(message);
                                int agencyId = job.getJob().getAgencyId();
                                String bibliographicRecordId = job.getJob().getBibliographicRecordId();
                                String queueName = entry.getKey();
                                String worker = queueRules.get(queueName);
                                try {
                                    stmt.setString(1, bibliographicRecordId);
                                    stmt.setInt(2, agencyId);
                                    stmt.setString(3, worker);
                                    stmt.setTimestamp(4, Timestamp.from(Instant.ofEpochMilli(message.getJMSTimestamp())));
                                    stmt.executeUpdate();
                                    processedCounter.inc();
                                    log.info("Moved " + agencyId + ":" + bibliographicRecordId + " from " + queueName + " to " + worker);
                                    message.acknowledge();
                                } catch (SQLException ex) {
                                    log.error("Exception: " + ex.getMessage());
                                    log.debug("Exception:", ex);
                                    log.info("Moved " + agencyId + ":" + bibliographicRecordId + " from " + queueName + " to " + worker);
                                    if (jmsContext.getTransacted()) {
                                        jmsContext.rollback();
                                    }
                                    throw ex;
                                }
                            }
                        }
                        if (gotMessage) {
                            if (jmsContext.getTransacted()) {
                                jmsContext.commit();
                            }
                            connection.commit();
                            connection.setAutoCommit(false);
                        } else {
                            Thread.sleep(1000L * cfg.getPollInterval());
                        }
                    }
                } finally {
                    for (JMSConsumer consumer : consumers.values()) {
                        try {
                            consumer.close();
                        } catch (Exception ex) {
                            log.error("Error closing consumer: " + ex.getMessage());
                            log.debug("Error closing consumer:", ex);
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
