/*
 * Copyright (C) 2015 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-agency-delete
 *
 * dbc-rawrepo-agency-delete is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-agency-delete is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.agencydelete;

import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.rawrepo.AgencySearchOrderFallback;
import dk.dbc.rawrepo.QueueJob;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class AgencyDeleteIT {

    private String jdbcUrl;
    private Connection connection;

    public AgencyDeleteIT() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws SQLException, RawRepoException, UnsupportedEncodingException {
        String port = System.getProperty("postgresql.port");
        jdbcUrl = "jdbc:postgresql://localhost:" + port + "/rawrepo";
        connection = DriverManager.getConnection(jdbcUrl);
        setupRecords();
    }

    @After
    public void tearDown() {
    }

//    @Test
    public void testGetIds() throws Exception {
        System.out.println("testGetIds()");
        AgencyDelete agencyDelete = new AgencyDelete(jdbcUrl, 191919);
        agencyDelete.begin();
        Set<String> ids = agencyDelete.getIds();
        testArray(ids, "Ids", "H", "S", "B", "E");

        Set<String> siblingRelations = agencyDelete.getSiblingRelations();
        testArray(siblingRelations, "SiblingRelations", "H", "S", "B");
        Set<String> parentRelations = agencyDelete.getParentRelations();
        testArray(parentRelations, "parentRelations", "H", "S");
        agencyDelete.commit();
    }

    @Test
    public void testBasics() throws Exception {
        System.out.println("testBasics()");
        AgencyDelete agencyDelete = new AgencyDelete(jdbcUrl, 777777);

        agencyDelete.begin();
        Set<String> ids = agencyDelete.getIds();
        testArray(ids, "Ids", "H", "S", "B", "E");

        Set<String> parentRelations = agencyDelete.getParentRelations();

        agencyDelete.queueRecords(ids, parentRelations, "provider");
        System.out.println("queued records");
        agencyDelete.removeRelations();
        System.out.println("removed relations");
        agencyDelete.deleteRecords(ids, parentRelations);
        System.out.println("deleted records");
        agencyDelete.commit();

        RawRepoDAO dao = RawRepoDAO.newInstance(connection, new AgencySearchOrderFallback("191919"));

        countQueued("leaf", 2);
        HashSet<String> leafs = new HashSet<>();
        for (QueueJob dequeue = dao.dequeue("leaf") ; dequeue != null ; dequeue = dao.dequeue("leaf")) {
            leafs.add(dequeue.getJob().getBibliographicRecordId());
        }
        testArray(leafs, "leafs", "B", "E");

        HashSet<String> nodes = new HashSet<>();
        countQueued("node", 2);
        System.out.println("nodes = " + nodes);
        for (QueueJob dequeue = dao.dequeue("node") ; dequeue != null ; dequeue = dao.dequeue("node")) {
            nodes.add(dequeue.getJob().getBibliographicRecordId());
        }
        testArray(nodes, "nodes", "H", "S");
        System.out.println("Done!");
        connection.commit();
    }

    @Test
    public void testQueue() throws Exception {
        {
            RawRepoDAO dao = RawRepoDAO.newInstance(connection, new AgencySearchOrderFallback("191919"));
            connection.setAutoCommit(false);
            setupRecord(dao, "S", 888888, "S:191919");
            connection.commit();
        }
        AgencyDelete agencyDelete = new AgencyDelete(jdbcUrl, 888888);
        agencyDelete.begin();
        Set<String> ids = agencyDelete.getIds();

        Set<String> parentRelations = agencyDelete.getParentRelations();

        agencyDelete.queueRecords(ids, parentRelations, "provider");
        agencyDelete.removeRelations();
        agencyDelete.deleteRecords(ids, parentRelations);
        agencyDelete.commit();

        countQueued("node", 1);
        countQueued("leaf", 1);

    }

    private void setupRecords() throws RawRepoException, SQLException, UnsupportedEncodingException {
        RawRepoDAO dao = RawRepoDAO.newInstance(connection, new AgencySearchOrderFallback("191919"));
        connection.setAutoCommit(false);
        connection.prepareStatement("DELETE FROM relations").execute();
        connection.prepareStatement("DELETE FROM records").execute();
        connection.prepareStatement("DELETE FROM queue").execute();
        connection.prepareStatement("DELETE FROM queuerules").execute();
        connection.prepareStatement("DELETE FROM queueworkers").execute();

        PreparedStatement stmt = connection.prepareStatement("INSERT INTO queueworkers(worker) VALUES(?)");
        stmt.setString(1, "leaf");
        stmt.execute();
        stmt.setString(1, "node");
        stmt.execute();

        stmt = connection.prepareStatement("INSERT INTO queuerules(provider, worker, mimetype, changed, leaf) VALUES(?, ?, '', 'A', ?)");
        stmt.setString(1, "provider");
        stmt.setString(2, "leaf");
        stmt.setString(3, "Y");
        stmt.execute();
        stmt.setString(1, "provider");
        stmt.setString(2, "node");
        stmt.setString(3, "N");
        stmt.execute();

        setupRecord(dao, "H", 191919);
        setupRecord(dao, "S", 191919, "H:191919");
        setupRecord(dao, "B", 191919, "S:191919");
        setupRecord(dao, "E", 191919);

        setupRecord(dao, "H", 777777, "H:191919");
        setupRecord(dao, "S", 777777, "S:191919");
        setupRecord(dao, "B", 777777, "B:191919");
        setupRecord(dao, "E", 777777);

        connection.commit();

    }

    private void setupRecord(RawRepoDAO dao, String bibliographicRecordId, int agencyId, String... relations) throws RawRepoException, UnsupportedEncodingException {
        System.out.println("bibliographicRecordId = " + bibliographicRecordId + "; agencyId = " + agencyId);
        Record rec = dao.fetchRecord(bibliographicRecordId, agencyId);
        rec.setContent(( bibliographicRecordId + ":" + agencyId ).getBytes("UTF-8"));
        rec.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
        rec.setDeleted(false);
        Set<RecordId> relationSet = new HashSet<>();
        for (String relation : relations) {
            String[] split = relation.split(":", 2);
            if (bibliographicRecordId.equals(split[0])) {
                rec.setMimeType(MarcXChangeMimeType.ENRICHMENT);
            }
            relationSet.add(new RecordId(split[0], Integer.parseInt(split[1])));
        }
        dao.saveRecord(rec);
        dao.setRelationsFrom(rec.getId(), relationSet);
    }

    private void testArray(Set<String> ids, String message, String... strings) {
        assertEquals(message + " size mismatch", ids.size(), strings.length);
        for (String string : strings) {
            assertTrue(message + " has " + string, ids.contains(string));
        }
    }

    private void countQueued(String queue, int count) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("SELECT COUNT(*) FROM queue WHERE worker=?");
        stmt.setString(1, queue);
        ResultSet resultSet = stmt.executeQuery();
        resultSet.next();
        int fromQueue = resultSet.getInt(1);
        assertEquals("queue: " + queue, count, fromQueue);
    }

}
