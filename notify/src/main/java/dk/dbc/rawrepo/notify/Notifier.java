/*
 This file is part of opensearch.
 Copyright © 2013, Dansk Bibliotekscenter a/s,
 Tempovej 7-11, DK-2750 Ballerup, Denmark. CVR: 15149043

 opensearch is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 opensearch is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with opensearch.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.notify;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import dk.dbc.rawrepo.QueueJob;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import java.sql.Connection;
import java.sql.SQLException;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.sql.DataSource;
import javax.xml.parsers.SAXParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Stateless
public class Notifier {

    private final static Logger log = LoggerFactory.getLogger(Notifier.class);

    @Resource(lookup = "jdbc/rawreponotifier/rawrepo")
    DataSource dataSource;

    @Resource(name = "workerName")
    String workerName;

    @Inject
    MetricsRegistry registry;

    Timer processJobTimer;
    Timer getConnectionTimer;
    Timer createDAOTimer;
    Timer dequeueJobTimer;
    Timer fetchRecordTimer;
    Timer queueSuccessTimer;
    Timer queueFailTimer;
    Timer commitTimer;

    SAXParser parser;

    public Notifier() {
    }

    @PostConstruct
    public void create() {
        log.info("Initializing");

        processJobTimer = registry.getRegistry().timer(MetricRegistry.name(Notifier.class, "processJob"));
        getConnectionTimer = registry.getRegistry().timer(MetricRegistry.name(Notifier.class, "getConnection"));
        createDAOTimer = registry.getRegistry().timer(MetricRegistry.name(Notifier.class, "createDAO"));
        dequeueJobTimer = registry.getRegistry().timer(MetricRegistry.name(Notifier.class, "dequeueJob"));
        fetchRecordTimer = registry.getRegistry().timer(MetricRegistry.name(Notifier.class, "fetchRecord"));
        queueSuccessTimer = registry.getRegistry().timer(MetricRegistry.name(Notifier.class, "queueSuccess"));
        queueFailTimer = registry.getRegistry().timer(MetricRegistry.name(Notifier.class, "queueFail"));
        commitTimer = registry.getRegistry().timer(MetricRegistry.name(Notifier.class, "commit"));

    }

    @PreDestroy
    public void destroy() {

    }

    public void performWork() {
        log.info("Processing available jobs from {}", workerName);

        boolean moreWork = true;
        int processedJobs = 0;

        while (moreWork) {

            Timer.Context time = processJobTimer.time();
            try (Connection connection = getConnection()) {
                RawRepoDAO dao = createDAO(connection);
                QueueJob job = dequeueJob(dao);

                if (job != null) {
                    processJob(job, dao);
                    commit(connection);
                    processedJobs++;
                    time.stop();
                } else {
                    moreWork = false;
                    log.trace("Queue is empty. Nothing to process");
                }
            } catch (RawRepoException | SQLException ex) {
                moreWork = false;
                log.error("Error getting job from database", ex);
            }
        }
        log.info("Done processing {} jobs from {}", processedJobs, workerName);
    }

    protected Connection getConnection() throws SQLException {
        Timer.Context time = getConnectionTimer.time();
        final Connection connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        time.stop();
        return connection;
    }

    private RawRepoDAO createDAO(final Connection connection) throws RawRepoException {
        Timer.Context time = createDAOTimer.time();
        final RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        time.stop();
        return dao;
    }

    private void processJob(QueueJob job, RawRepoDAO dao) throws RawRepoException {
        log.debug("Processing {}", job);
        //TODO notify someone about something

        queueSuccess(dao, job);
    }

    private QueueJob dequeueJob(final RawRepoDAO dao) throws RawRepoException {
        Timer.Context time = dequeueJobTimer.time();
        final QueueJob job = dao.dequeue(workerName);
        if (job != null) {
            time.stop();
        }
        return job;
    }

    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private Record fetchRecord(RawRepoDAO dao, String id, int library) throws RawRepoException {
        Timer.Context time = fetchRecordTimer.time();
        Record record = dao.fetchRecord(id, library);
        time.stop();
        //log.trace("{} '{}', '{}'", record, record.getContent(), new String(record.getContent()));
        return record;
    }

    private void queueSuccess(RawRepoDAO dao, QueueJob job) throws RawRepoException {
        Timer.Context time = queueSuccessTimer.time();
        dao.queueSuccess(job);
        time.stop();
    }

    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private void queueFail(RawRepoDAO dao, QueueJob job, String id) throws RawRepoException {
        Timer.Context time = queueFailTimer.time();
        dao.queueFail(job, id);
        time.stop();
    }

    private void commit(final Connection connection) throws SQLException {
        Timer.Context time = commitTimer.time();
        connection.commit();
        time.stop();
    }
}
