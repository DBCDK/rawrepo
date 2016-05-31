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

import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.Record;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;

import org.junit.Before;
import org.postgresql.ds.PGSimpleDataSource;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class NotifierIT {

    private static final String PROVIDER = "test";
    private static final String WORKER = "changed";

    String jdbcUrl;
    Connection connection;

    @Before
    public void setUp() throws Exception {
        String port = System.getProperty("postgresql.port");
        jdbcUrl = "jdbc:postgresql://localhost:" + port + "/rawrepo";
        connection = DriverManager.getConnection(jdbcUrl);
        connection.setAutoCommit(false);
        resetDatabase();
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
    }

    void resetDatabase() throws SQLException {
        connection.prepareStatement("DELETE FROM relations").execute();
        connection.prepareStatement("DELETE FROM records").execute();
        connection.prepareStatement("DELETE FROM queue").execute();
        connection.prepareStatement("DELETE FROM queuerules").execute();
        connection.prepareStatement("DELETE FROM queueworkers").execute();

        PreparedStatement stmt = connection.prepareStatement("INSERT INTO queueworkers(worker) VALUES(?)");
        stmt.setString(1, WORKER);
        stmt.execute();

        stmt = connection.prepareStatement("INSERT INTO queuerules(provider, worker, changed, leaf) VALUES('" + PROVIDER + "', ?, ?, ?)");
        stmt.setString(1, WORKER);
        stmt.setString(2, "Y");
        stmt.setString(3, "A");
        stmt.execute();
    }

    private int getQueueSize() throws SQLException {
        ResultSet rs = connection.prepareStatement("SELECT COUNT(*) AS total FROM queue").executeQuery();
        rs.next();
        return rs.getInt("total");
    }

    private Notifier createInstance() throws SQLException {
        Notifier notifier = new Notifier();
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(jdbcUrl);
        notifier.dataSource = dataSource;

        notifier.workerName = WORKER;
        notifier.registry = new MetricsRegistry();
        notifier.create();
        return notifier;
    }

    @Test
    public void testPerformWork() throws Exception {
        // Create dao
        RawRepoDAO dao = RawRepoDAO.builder(connection).build();
        assertFalse(dao.recordExists("A", 870970));
        assertEquals(0, getQueueSize());

        // Store a record
        Record record1 = dao.fetchRecord("A", 870970);
        record1.setContent("First edition".getBytes());
        record1.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
        dao.saveRecord(record1);
        assertTrue(dao.recordExists("A", 870970));
        dao.changedRecord(PROVIDER, record1.getId(), "text/plain");
        connection.commit();
        assertEquals(1, getQueueSize());

        // Let the notifer perform work
        Notifier notifier = createInstance();
        notifier.performWork();

        // Check that queue has shrinked
        assertEquals(0, getQueueSize());

    }
}
