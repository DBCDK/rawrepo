/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dk.dbc.rawrepo.notify;

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
 * @author kasper
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
        RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        assertFalse(dao.recordExists("A", 870970));
        assertEquals(0, getQueueSize());

        // Store a record
        Record record1 = dao.fetchRecord("A", 870970);
        record1.setContent("First edition".getBytes());
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
