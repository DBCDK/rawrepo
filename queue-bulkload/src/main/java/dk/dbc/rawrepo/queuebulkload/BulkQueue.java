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

import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.RecordId;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
public class BulkQueue {

    private static final Logger log = LoggerFactory.getLogger(BulkQueue.class);

    String db;
    Integer commit;
    String role;
    Connection connection;
    RawRepoDAO dao;

    public BulkQueue(String db, Integer commit, String role) throws SQLException, RawRepoException {
        this.db = db;
        this.commit = commit;
        this.role = role;
        dao = openDatabase(db);
    }

    public void run(Iterator<RecordId> iterator, String fallbackMimeType) {
        try {
            int row = 0;
            connection.setAutoCommit(false);
            while (iterator.hasNext()) {
                if (row == commit) {
                    log.debug("commit");
                    connection.commit();
                    connection.setAutoCommit(false);
                    row = 0;
                }
                row++;
                RecordId id = iterator.next();
                log.debug("id = " + id);
                dao.changedRecord(role, id, fallbackMimeType);
            }
            connection.commit();
        } catch (Exception ex) {
            log.error("Caught exception:", ex);
            try {
                connection.rollback();
            } catch (SQLException ex1) {
                log.error("Rolling back - Caught exception:", ex1);
            }
        }
    }

    public void run(Iterator<RecordId> iterator) {
        try {
            connection.setAutoCommit(false);
            try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO queue (bibliographicrecordid, agencyid, worker) VALUES(?, ?, ?)")) {
                stmt.setString(3, role);
                int row = 0;
                while (iterator.hasNext()) {
                    if (row == commit) {
                        log.debug("commit");
                        connection.commit();
                        connection.setAutoCommit(false);
                        row = 0;
                    }
                    row++;
                    RecordId next = iterator.next();
                    stmt.setString(1, next.getBibliographicRecordId());
                    stmt.setInt(2, next.getAgencyId());
                    stmt.execute();
                }
                connection.commit();
            }
        } catch (SQLException ex) {
            log.error("Caught exception:", ex);
            try {
                connection.rollback();
            } catch (SQLException ex1) {
                log.error("Rolling back - Caught exception:", ex1);
            }
        }
    }

    private static final Pattern urlPattern = Pattern.compile("^(jdbc:[^:]*://)?(?:([^:@]*)(?::([^@]*))?@)?((?:([^:/]*)(?::(\\d+))?)(?:/(.*))?)$");
    private static final String jdbcDefault = "jdbc:postgresql://";
    private static final int urlPatternPrefix = 1;
    private static final int urlPatternUser = 2;
    private static final int urlPatternPassword = 3;
    private static final int urlPatternHostPortDb = 4;

    private RawRepoDAO openDatabase(String url) throws SQLException, RawRepoException {
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
        connection = DriverManager.getConnection(jdbc + matcher.group(urlPatternHostPortDb), properties);
        log.debug("Connected");
        return RawRepoDAO.builder(connection).build();
    }
}
