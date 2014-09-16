/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-commons
 *
 * dbc-rawrepo-commons is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-commons is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import org.xml.sax.SAXException;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class RawRepoDAOIT {

    private Connection connection;
    private String jdbc;

    @Before
    public void setup() throws SQLException, ClassNotFoundException {
        String port = System.getProperty("postgresql.port");
        jdbc = "jdbc:postgresql://localhost:" + port + "/rawrepo";
        connection = DriverManager.getConnection(jdbc);
        resetDatabase();
    }

    @After
    public void teardown() throws SQLException {
        connection.close();
    }

    @Test
    public void testReadWriteRecord() throws SQLException, ClassNotFoundException, RawRepoException {
        setupData("A:870970");
        RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        connection.setAutoCommit(false);

        //
        // Check recordExists()
        //
        assertTrue(dao.recordExists("A", 870970));
        assertFalse(dao.recordExists("a", 870970));

        //
        // Fetch a record (create)
        // Change it
        // Save
        // Check original status
        //
        Record record1 = dao.fetchRecord("B", 870970);
        assertTrue(record1.isOriginal());
        record1.setContent("First edition".getBytes());
        dao.saveRecord(record1);
        assertFalse(record1.isOriginal());

        Record recordTest1 = dao.fetchRecord("B", 870970);

        //
        // Fetch a record (from db)
        // Change it
        // Save
        // Check original status
        //
        Record record2 = dao.fetchRecord("B", 870970);
        assertFalse(record2.isOriginal());
        record2.setContent("Second Edition".getBytes());
        dao.saveRecord(record2);

        Record recordTest2 = dao.fetchRecord("B", 870970);

        //
        // Validate created, modified and content for the 2 records
        //
        assertFalse(recordTest2.getModified().equals(recordTest1.getModified()));
        assertTrue(recordTest2.getCreated().equals(recordTest1.getCreated()));
        assertFalse(new String(recordTest2.getContent()).equals(new String(recordTest1.getContent())));

        connection.commit();
    }

    @Test
    public void testFetchRecordCollection() throws RawRepoException, MarcXMergerException, SQLException, ClassNotFoundException {
        setupData("B:2", "B:870970", "C:870970", "D:1", "D:870970", "E:870970", "F:1", "F:870970", "G:870970", "H:1", "H:870970");
        RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        connection.setAutoCommit(false);

        MarcXMerger merger = new MarcXMerger() {

            @Override
            public byte[] merge(byte[] common, byte[] local) throws MarcXMergerException {
                return local;
            }

        };

        collectionIs(idsFromCollection(dao.fetchRecordCollection("D", 870970, merger)), "B:870970", "C:870970", "D:870970");
        System.out.println("------------------------------------------------");
        collectionIs(idsFromCollection(dao.fetchRecordCollection("D", 1, merger)), "B:870970", "C:870970", "D:1");
        System.out.println("------------------------------------------------");

        collectionIs(idsFromCollection(dao.fetchRecordCollection("G", 1, merger)), "B:870970", "F:1", "G:870970");

        collectionIs(idsFromCollection(dao.fetchRecordCollection("H", 1, merger)), "B:870970", "F:1", "H:1");

        collectionIs(idsFromCollection(dao.fetchRecordCollection("H", 2, merger)), "B:2", "F:870970", "H:870970");

        connection.commit();

    }

    @Test
    public void testFetchRecordCollectionNoCommonLibrary() throws SQLException, RawRepoException, MarcXMergerException {
        setupData("B:1", "C:1", "D:1", "E:1", "F:1", "G:1", "H:1");
        setupRelations("C:1,B:1", "D:1,C:1", "E:1,C:1", "F:1,B:1", "G:1,F:1", "H:1,F:1");
        RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        connection.setAutoCommit(false);
        MarcXMerger merger = new MarcXMerger();
        collectionIs(idsFromCollection(dao.fetchRecordCollection("D", 1, merger)), "B:1", "C:1", "D:1");
    }

    @Test
    public void testDeleteRecord() throws SQLException, RawRepoException {
        setupData("A:870970", "B:870970");
        RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        connection.setAutoCommit(false);

        //Ensure record is in base
        Record recA = dao.fetchRecord("A", 870970);
        assertFalse(recA.isOriginal());
        assertTrue(recA.hasContent());

        // Delete Record
        dao.deleteRecord(recA.getId());

        // Record Doesn't exist
        assertFalse(dao.recordExists("A", 870970));

        // Record is deleted
        recA = dao.fetchRecord("A", 870970);
        assertFalse(recA.isOriginal());
        assertFalse(recA.hasContent());

        // Purge Record
        dao.purgeRecord(recA.getId());

        // Record is newly created
        recA = dao.fetchRecord("A", 870970);
        assertTrue(recA.isOriginal());
        assertFalse(recA.hasContent());

        connection.commit();
    }

    @Test
    public void testQueueEntityWithout() throws SQLException, RawRepoException {
        setupData("A:870970");
        RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        connection.setAutoCommit(false);
        dao.changedRecord("test", recordIdFromString("A:870970"));
        collectionIs(getQueue(),
                     "A:870970:changed",
                     "A:870970:leaf");
        connection.commit();
    }

    @Test
    public void testQueueEntityWith() throws SQLException, RawRepoException {
        setupData("A:870970", "A:1", "A:2");
        RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        connection.setAutoCommit(false);
        dao.changedRecord("test", recordIdFromString("A:870970"));
        collectionIs(getQueue(),
                     "A:870970:changed",
                     "A:870970:leaf",
                     "A:1:leaf",
                     "A:2:leaf");
        connection.commit();
    }

    @Test
    public void testQueueEntityLocalData() throws SQLException, RawRepoException {
        setupData("A:870970", "A:1", "A:2");
        RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        connection.setAutoCommit(false);
        dao.changedRecord("test", recordIdFromString("A:1"));
        collectionIs(getQueue(),
                     "A:1:changed",
                     "A:1:leaf");
        connection.commit();
    }

    @Test
    public void testQueueSectionWithComplexLocal() throws SQLException, RawRepoException {
        setupData("B:870970", "B:1", // HEAD
                  "C:870970", // SECTION
                  "D:870970", // BIND
                  "E:870970", "E:2", // BIND
                  "F:870970", "F:2", // SECTION
                  "G:870970", "G:1", // BIND
                  "H:870970");// BIND
        RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        connection.setAutoCommit(false);
        dao.changedRecord("test", recordIdFromString("C:870970"));
        collectionIs(getQueue(),
                     "C:870970:changed", "C:870970:node", "C:1:node",
                     "D:870970:leaf", "D:1:leaf",
                     "E:870970:leaf", "E:1:leaf", "E:2:leaf");
        connection.commit();
    }

    @Test
    public void testQueueHeadWithComplexLocal() throws SQLException, RawRepoException {
        setupData("B:870970", // HEAD
                  "C:870970", "C:1", // SECTION
                  "D:870970", // BIND
                  "E:870970", "E:2", // BIND
                  "F:870970", "F:2", // SECTION
                  "G:870970", "G:1", // BIND
                  "H:870970");// BIND
        RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        connection.setAutoCommit(false);
        dao.changedRecord("test", recordIdFromString("B:870970"));
        collectionIs(getQueue(),
                     "B:870970:changed", "B:870970:node",
                     "C:870970:node", "C:1:node",
                     "D:870970:leaf", "D:1:leaf",
                     "E:870970:leaf", "E:1:leaf", "E:2:leaf",
                     "F:870970:node", "F:2:node",
                     "G:870970:leaf", "G:1:leaf", "G:2:leaf",
                     "H:870970:leaf", "H:2:leaf");
        connection.commit();
    }

    @Test
    public void testEnqueueWithChainedSiblings() throws SQLException, RawRepoException {
        setupData(
                "H:870970,1",
                "S1:870970,2,3",
                "B11:870970",
                "B12:870970,4,9" // 9 is local record not sibling
        );
        setupRelations(
                "H:1,H:870970",
                "S1:870970,H:870970", "S1:2,S1:870970", "S1:3,S1:2",
                "B11:870970,S1:870970",
                "B12:870970,S1:870970", "B12:4,B12:870970");
        RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        connection.setAutoCommit(false);

        clearQueue();
        dao.changedRecord("test", new RecordId("S1", 870970));
        collectionIs(getQueue(),
                     "S1:870970:changed",
                     "S1:870970:node", "S1:1:node", "S1:2:node", "S1:3:node",
                     "B11:870970:leaf", "B11:1:leaf", "B11:2:leaf", "B11:3:leaf",
                     "B12:870970:leaf", "B12:1:leaf", "B12:2:leaf", "B12:3:leaf", "B12:4:leaf");

        clearQueue();
        dao.changedRecord("test", new RecordId("S1", 2));
        collectionIs(getQueue(),
                     "S1:2:changed",
                     "S1:2:node", "S1:3:node",
                     "B11:2:leaf", "B11:3:leaf",
                     "B12:2:leaf", "B12:3:leaf");

        clearQueue();
        dao.changedRecord("test", new RecordId("B12", 9));
        collectionIs(getQueue(),
                     "B12:9:changed",
                     "B12:9:leaf");

        connection.commit();
    }

    @Test
    public void testDeQueueWithSavepoint() throws SQLException, RawRepoException {
        setupData();
        RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        connection.setAutoCommit(false);

        Connection connection1 = DriverManager.getConnection(jdbc);
        RawRepoDAO dao1 = RawRepoDAO.newInstance(connection1);
        connection1.setAutoCommit(false);

        Connection connection2 = DriverManager.getConnection(jdbc);
        RawRepoDAO dao2 = RawRepoDAO.newInstance(connection2);
        connection2.setAutoCommit(false);

        Connection connection3 = DriverManager.getConnection(jdbc);
        RawRepoDAO dao3 = RawRepoDAO.newInstance(connection3);
        connection3.setAutoCommit(false);

        System.out.println("QUEUE A:1");
        dao.enqueue(new RecordId("A", 1), "test", true, true);
        connection.commit();
        collectionIs(getQueueState(),
                     "A:1:changed::1", "A:1:leaf::1");

        System.out.println("TAKE A:1");
        QueueJob job1 = dao1.dequeueWithSavepoint("changed");

        System.out.println("QUEUE A:1 again");
        dao.enqueue(new RecordId("A", 1), "test", true, true);
        connection.commit();

        System.out.println("TEST");
        collectionIs(getQueueState(),
                     "A:1:changed::2", "A:1:leaf::1"); // one processing and one in queue

        System.out.println("TAKE NULL");
        QueueJob job2 = dao2.dequeueWithSavepoint("changed");
        assertNull(job2); // some in queue but one is processing

        System.out.println("QUEUE B:2");
        dao.enqueue(new RecordId("B", 2), "test", true, true); // 2 in queue
        connection.commit();

        System.out.println("TEST");
        collectionIs(getQueueState(),
                     "A:1:changed::2", "A:1:leaf::1",
                     "B:2:changed::1", "B:2:leaf::1");

        System.out.println("TAKE B:2");
        job2 = dao2.dequeueWithSavepoint("changed");
        assertNotNull(job2); //

        System.out.println("DONE A:1");
        dao1.queueFail(job1, "failure");
        connection1.commit(); // Commit 1st changed;

        System.out.println("DONE B:2");
        dao2.queueSuccess(job2);
        connection2.commit(); // Commit 1st changed;

        System.out.println("TEST");
        collectionIs(getQueueState(),
                     "A:1:changed:failure:1", "A:1:changed::1", "A:1:leaf::1",
                     "B:2:leaf::1");

        System.out.println("TAKE A:1#2");
        job1 = dao1.dequeueWithSavepoint("changed");
        assertNotNull(job1);

        System.out.println("DONE A:1#2");
        dao1.queueFail(job1, "failure");
        connection1.commit(); // Commit 1st changed;

        System.out.println("TEST");
        collectionIs(getQueueState(),
                     "A:1:changed:failure:2", "A:1:leaf::1",
                     "B:2:leaf::1");

        connection1.close();
        connection2.close();
        connection3.close();
        connection.commit();
    }

    @Test
    public void testDeQueue() throws SQLException, RawRepoException {
        setupData();
        RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        connection.setAutoCommit(false);

        dao.enqueue(new RecordId("A", 1), "test", true, true);
        connection.commit();
        collectionIs(getQueueState(),
                     "A:1:changed::1", "A:1:leaf::1");

        QueueJob job = dao.dequeue("changed");
        assertNotNull(job);
    }

//  _   _      _                   _____                 _   _
// | | | | ___| |_ __   ___ _ __  |  ___|   _ _ __   ___| |_(_) ___  _ __  ___
// | |_| |/ _ \ | '_ \ / _ \ '__| | |_ | | | | '_ \ / __| __| |/ _ \| '_ \/ __|
// |  _  |  __/ | |_) |  __/ |    |  _|| |_| | | | | (__| |_| | (_) | | | \__ \
// |_| |_|\___|_| .__/ \___|_|    |_|   \__,_|_| |_|\___|\__|_|\___/|_| |_|___/
//              |_|
    void resetDatabase() throws SQLException {
        connection.prepareStatement("DELETE FROM relations").execute();
        connection.prepareStatement("DELETE FROM records").execute();
        connection.prepareStatement("DELETE FROM queue").execute();
        connection.prepareStatement("DELETE FROM queuerules").execute();
        connection.prepareStatement("DELETE FROM queueworkers").execute();

        PreparedStatement stmt = connection.prepareStatement("INSERT INTO queueworkers(worker) VALUES(?)");
        stmt.setString(1, "changed");
        stmt.execute();
        stmt.setString(1, "leaf");
        stmt.execute();
        stmt.setString(1, "node");
        stmt.execute();

        stmt = connection.prepareStatement("INSERT INTO queuerules(provider, worker, changed, leaf) VALUES('test', ?, ?, ?)");
        stmt.setString(1, "changed");
        stmt.setString(2, "Y");
        stmt.setString(3, "A");
        stmt.execute();
        stmt.setString(1, "leaf");
        stmt.setString(2, "A");
        stmt.setString(3, "Y");
        stmt.execute();
        stmt.setString(1, "node");
        stmt.setString(2, "A");
        stmt.setString(3, "N");
        stmt.execute();
    }

    void setupData(String... ids) throws RawRepoException, SQLException {
        connection.setAutoCommit(false);
        RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        Map<String, Set<RecordId>> idMap = new HashMap<>();
        for (String id : ids) {
            idMap.put(id, new HashSet<RecordId>());
        }
        Set<String> keys = idMap.keySet();

        for (String id : keys) {
            String[] split1 = id.split(":");
            String[] split2 = split1[1].split(",");
            for (String lib : split2) {
                RecordId recordId = new RecordId(split1[0], Integer.parseInt(lib));
                Record record = dao.fetchRecord(recordId.getId(), recordId.getLibrary());
                record.setContent(id.getBytes());
                dao.saveRecord(record);
            }
        }
        setupRelations(RELATIONS);
        connection.commit();
    }

    void setupRelations(String... relations) throws NumberFormatException, RawRepoException, SQLException {
        connection.setAutoCommit(false);
        RawRepoDAO dao = RawRepoDAO.newInstance(connection);
        for (String relation : relations) {
            String[] list = relation.split(",", 2);
            RecordId from = recordIdFromString(list[0]);
            RecordId to = recordIdFromString(list[1]);
            if (dao.recordExists(from.getId(), from.getLibrary())
                && dao.recordExists(to.getId(), to.getLibrary())) {
                Set<RecordId> relationsFrom = dao.getRelationsFrom(from);
                relationsFrom.add(to);
                dao.setRelationsFrom(from, relationsFrom);
            }
        }
        connection.commit();
    }

    void clearQueue() throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("DELETE FROM QUEUE");
        stmt.execute();
    }

    Collection<String> getQueue() throws SQLException {
        Set<String> result = new HashSet<>();
        PreparedStatement stmt = connection.prepareStatement("SELECT id, library, worker FROM QUEUE");
        if (stmt.execute()) {
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                result.add(resultSet.getString(1) + ":" + resultSet.getInt(2) + ":" + resultSet.getString(3));
            }
        }
        return result;
    }

    Collection<String> getQueueState() throws SQLException {
        Set<String> result = new HashSet<>();
        PreparedStatement stmt = connection.prepareStatement("SELECT id, library, worker, blocked, COUNT(queued) FROM QUEUE GROUP BY id, library, worker, blocked");
        if (stmt.execute()) {
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                result.add(resultSet.getString(1) + ":" + resultSet.getInt(2) + ":" + resultSet.getString(3) + ":" + resultSet.getString(4) + ":" + resultSet.getInt(5));
            }
        }
        return result;
    }

    public Collection<String> idsFromCollection(Map<String, Record> records) {
        Collection<String> collection = new HashSet<>();
        for (Record record : records.values()) {
            collection.add(record.getId().getId() + ":" + record.getId().getLibrary());
        }
        return collection;
    }

    /**
     * Raise an (descriptive) exception if a collection of strings doesn't match supplied list
     *
     * @param col collection
     * @param elems string elements collection should consist of
     */
    private static void collectionIs(Collection<String> col, String... elems) {
        HashSet<String> missing = new HashSet();
        Collections.addAll(missing, elems);
        HashSet<String> extra = new HashSet(col);
        extra.removeAll(missing);
        missing.removeAll(col);
        if (!extra.isEmpty() || !missing.isEmpty()) {
            throw new RuntimeException("missing:" + missing.toString() + ", extra=" + extra.toString());
        }
    }

    /**
     * Parse a string to a recordid
     *
     * @param target ID:LIBRARY
     * @return recordid
     * @throws NumberFormatException
     */
    private static RecordId recordIdFromString(String target) throws NumberFormatException {
        String[] list = target.split(":");
        return new RecordId(list[0], Integer.parseInt(list[1]));
    }

    /*
     * (e) A
     *
     * (h) B
     * (s)  C
     * (b)   D
     * (b)   E
     * (s)  F
     * (b)   G
     * (b)   H
     */
    private static final String[] RELATIONS = new String[]{
        "A:1,A:870970",
        "A:2,A:870970",
        "B:1,B:870970",
        "B:2,B:870970",
        "C:1,C:870970",
        "C:2,C:870970",
        "C:870970,B:870970",
        "D:1,D:870970",
        "D:2,D:870970",
        "D:870970,C:870970",
        "E:1,E:870970",
        "E:2,E:870970",
        "E:870970,C:870970",
        "F:1,F:870970",
        "F:2,F:870970",
        "F:870970,B:870970",
        "G:1,G:870970",
        "G:2,G:870970",
        "G:870970,F:870970",
        "H:1,H:870970",
        "H:2,H:870970",
        "H:870970,F:870970"};

}
