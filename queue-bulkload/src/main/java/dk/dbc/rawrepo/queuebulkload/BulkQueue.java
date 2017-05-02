/*
 * dbc-rawrepo-queue-bulkload
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-queue-bulkload.
 *
 * dbc-rawrepo-queue-bulkload is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-queue-bulkload is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-queue-bulkload.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.queuebulkload;

import com.sun.messaging.ConnectionConfiguration;
import com.sun.messaging.ConnectionFactory;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.QueueJob;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.RecordId;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class BulkQueue {

    private static final Logger log = LoggerFactory.getLogger(BulkQueue.class);

    final String db;
    final Integer commit;
    final String role;
    final Connection connection;
    final RawRepoDAO dao;
    final JMSContext context;

    public BulkQueue(String db, String mq, int retryInterval, int retryCount, String openagency, Integer commit, String role) throws SQLException, RawRepoException, JMSException {
        this.db = db;
        this.commit = commit;
        this.role = role;
        this.connection = openDatabase(db);
        RawRepoDAO.Builder builder = RawRepoDAO.builder(connection);

        if (openagency != null) {
            OpenAgencyServiceFromURL service = OpenAgencyServiceFromURL.builder()
                    .build(openagency);
            builder.openAgency(service, null);
        }

        if (mq != null) {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setProperty(ConnectionConfiguration.imqAddressList, mq);
            this.context = connectionFactory.createContext(Session.AUTO_ACKNOWLEDGE);
            builder.queue(context, retryCount, retryInterval);
        } else {
            this.context = null;
        }
        this.dao = builder
                .build();
    }

    public void close() {
        if (context != null) {
            context.close();
        }
    }

    public void run(Iterator<RecordId> iterator) {
        try {
            int row = 0;
            while (iterator.hasNext()) {
                if (row == commit) {
                    dao.commitQueue();
                    row = 0;
                }
                row++;
                RecordId id = iterator.next();
                log.debug("id = " + id);
                dao.changedRecord(role, id);
            }
            dao.commitQueue();
        } catch (RawRepoException | JMSException ex) {
            log.error("Caught exception:", ex);
            try {
                connection.rollback();
            } catch (SQLException ex1) {
                log.error("Rolling back - Caught exception:", ex1);
            }
        }
    }

    public void runNoRule(Iterator<RecordId> iterator) {
        try {
            List<String> queues = Arrays.asList(role);
            int row = 0;
            while (iterator.hasNext()) {
                if (row == commit) {
                    log.debug("commit");
                    dao.commitQueue();
                    row = 0;
                }
                row++;
                RecordId next = iterator.next();
                dao.queue(new QueueJob(next.getBibliographicRecordId(), next.getAgencyId()), queues);
            }
            dao.commitQueue();
        } catch (JMSException ex) {
            log.error("Caught jms exception: " + ex.getMessage());
            log.debug("Caught jms exception:", ex);
        }
    }

    private static final Pattern urlPattern = Pattern.compile("^(jdbc:[^:]*://)?(?:([^:@]*)(?::([^@]*))?@)?((?:([^:/]*)(?::(\\d+))?)(?:/(.*))?)$");
    private static final String jdbcDefault = "jdbc:postgresql://";
    private static final int urlPatternPrefix = 1;
    private static final int urlPatternUser = 2;
    private static final int urlPatternPassword = 3;
    private static final int urlPatternHostPortDb = 4;

    private static Connection openDatabase(String url) throws SQLException, RawRepoException {
        Matcher matcher = urlPattern.matcher(url);
        if (!matcher.find()) {
            throw new IllegalArgumentException(url + " Is not a valid jdbc uri");
        }
        Properties properties = new Properties();
        String jdbc = matcher.group(urlPatternPrefix);
        if (jdbc == null) {
            jdbc = jdbcDefault;
        }
        if (matcher.group(urlPatternUser) != null) {
            properties.setProperty("user", matcher.group(urlPatternUser));
        }
        if (matcher.group(urlPatternPassword) != null) {
            properties.setProperty("password", matcher.group(urlPatternPassword));
        }

        log.debug("Connecting");
        Connection con = DriverManager.getConnection(jdbc + matcher.group(urlPatternHostPortDb), properties);
        log.debug("Connected");
        return con;
    }
}
