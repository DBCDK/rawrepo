/*
 * dbc-rawrepo-access
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-access.
 *
 * dbc-rawrepo-access is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-access is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-access.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo;

import dk.dbc.commons.testutils.postgres.connection.PostgresITConnection;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class RawRepoDAOIT {
    private static final XLogger logger = XLoggerFactory.getXLogger(RawRepoDAOIT.class);

    private Connection connection;
    private PostgresITConnection postgres;

    private MarcXMerger merger;

    @BeforeEach
    void setup() throws SQLException {
        postgres = new PostgresITConnection("rawrepo");
        connection = postgres.getConnection();
        // Don't do this unless you know what you are doing. You need to be superuser in the database
        // before you can do it. Only effect seems to be that sql statements are written to the pg logfile.
        // connection.prepareStatement("SET log_statement = 'all';").execute();
        resetDatabase();
    }

    @AfterEach
    void teardown() throws SQLException {
        postgres.close();
    }

    @Test
    public void testReadWriteRecord() throws SQLException, RawRepoException, InterruptedException {
        setupData(100000, "A:870970");
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();

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
        record1.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
        dao.saveRecord(record1);
        assertFalse(record1.isOriginal());

        Record recordTest1 = dao.fetchRecord("B", 870970);

        //
        // Fetch a record (from db)
        // Change it
        // Save
        // Check original status
        //
        Thread.sleep(1);
        Record record2 = dao.fetchRecord("B", 870970);
        assertFalse(record2.isOriginal());
        record2.setContent("Second Edition".getBytes());
        dao.saveRecord(record2);

        Record recordTest2 = dao.fetchRecord("B", 870970);

        //
        // Validate created, modified and content for the 2 records
        //
        assertNotEquals(recordTest1.getModified(), recordTest2.getModified());
        assertEquals(recordTest1.getCreated(), recordTest2.getCreated());
        assertNotEquals(new String(recordTest1.getContent()), new String(recordTest2.getContent()));

        connection.commit();
        connection.setAutoCommit(false);
        Record record12 = dao.fetchRecord("C", 12);
        record12.setDeleted(false);
        record12.setContent("HELLO".getBytes());
        record12.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
        dao.saveRecord(record12);
        connection.commit();
        connection.setAutoCommit(false);

        boolean recordExists = dao.recordExists("C", 12);
        logger.info("recordExists = " + recordExists);
        assertTrue(recordExists);
    }

    @Test
    public void testHistoricRecord() throws SQLException, RawRepoException {
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);
        Record record;
        record = dao.fetchRecord("a bcd efg h", 100000);
        record.setContent("Version 1".getBytes());
        record.setMimeType("text/plain");
        record.setDeleted(false);
        dao.saveRecord(record);
        connection.commit();

        connection.setAutoCommit(false);
        record = dao.fetchRecord("a bcd efg h", 100000);
        record.setContent("Version 2".getBytes());
        record.setMimeType("text/not-so-plain");
        record.setDeleted(true);
        dao.saveRecord(record);
        connection.commit();

        connection.setAutoCommit(false);
        record = dao.fetchRecord("a bcd efg h", 100000);
        record.setContent("Version 3".getBytes());
        record.setMimeType("text/really-plain");
        record.setDeleted(false);
        dao.saveRecord(record);
        connection.commit();

        List<RecordMetaDataHistory> recordHistory = dao.getRecordHistory("a bcd efg h", 100000);
        assertEquals(recordHistory.size(), 3);

        assertFalse(recordHistory.get(0).isDeleted());
        assertTrue(recordHistory.get(1).isDeleted());
        assertFalse(recordHistory.get(2).isDeleted());

        assertThat("newest mimetype", recordHistory.get(0).getMimeType(), is("text/really-plain"));
        assertThat("modified mimetype", recordHistory.get(1).getMimeType(), is("text/not-so-plain"));
        assertThat("oldest mimetype", recordHistory.get(2).getMimeType(), is("text/plain"));

        assertThat("newest content", dao.getHistoricRecord(recordHistory.get(0)).getContent(), is("Version 3".getBytes()));
        assertThat("modified content", dao.getHistoricRecord(recordHistory.get(1)).getContent(), is("Version 2".getBytes()));
        assertThat("oldest content", dao.getHistoricRecord(recordHistory.get(2)).getContent(), is("Version 1".getBytes()));
    }

    @Test
    public void testFetchRecordSchoolLibrary() throws Exception {
        setupData(0, "A:300041", "A:300000", "A:870970");
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);

        collectionIs(idsFromCollection(dao.fetchRecordCollection("A", 300041, getMarcXMerger())), "A:300041");
        collectionIs(idsFromCollection(dao.fetchRecordCollection("A", 300042, getMarcXMerger())), "A:300000");
        collectionIs(idsFromCollection(dao.fetchRecordCollection("A", 870970, getMarcXMerger())), "A:870970");
    }

    @Test
    public void testFetchRecordKKB() throws Exception {
        setupData(0, "A:810014", "A:810010", "A:870970");
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);

        collectionIs(idsFromCollection(dao.fetchRecordCollection("A", 810014, getMarcXMerger())), "A:810014");
        collectionIs(idsFromCollection(dao.fetchRecordCollection("A", 810010, getMarcXMerger())), "A:810010");
        collectionIs(idsFromCollection(dao.fetchRecordCollection("A", 870970, getMarcXMerger())), "A:870970");
    }

    @Test
    public void testFetchRecordCollectionDataIO() throws Exception {
        setupData(200000, "A:191919", "A:870970");
        setupRelations("A:191919, A:870970");
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);

        collectionIs(idsFromCollection(dao.fetchRecordCollection("A", 191919, getMarcXMerger())), "A:191919");
    }

    @Test
    public void testFetchRecordCollectionMoreAuthority() throws Exception {
        setupData(820000, "A:191919", "A:870970", "B:870979", "A:810010", "A:810014");
        setupRelations("A:191919, A:870970", "A:870970,B:870979", "A:810010, A:870970", "A:810014,A:870970");
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);

        collectionIs(idsFromCollection(dao.fetchRecordCollection("A", 191919, getMarcXMerger())), "A:191919", "B:870979");
        collectionIs(idsFromCollection(dao.fetchRecordCollection("A", 870970, getMarcXMerger())), "A:870970", "B:870979");
        collectionIs(idsFromCollection(dao.fetchRecordCollection("A", 810010, getMarcXMerger())), "A:810010", "B:870979");
        collectionIs(idsFromCollection(dao.fetchRecordCollection("A", 810014, getMarcXMerger())), "A:810014", "B:870979");
    }

    @Test
    public void testFetchRecordCollection() throws RawRepoException, MarcXMergerException, SQLException {
        setupData(100000, "B:2,870970", "C:870970", "D:1,870970", "E:870970", "F:1,870970", "G:870970", "H:1,870970");
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);

        collectionIs(idsFromCollection(dao.fetchRecordCollection("D", 870970, getMarcXMerger())), "B:870970", "C:870970", "D:870970");

        collectionIs(idsFromCollection(dao.fetchRecordCollection("D", 1, getMarcXMerger())), "B:870970", "C:870970", "D:1");

        collectionIs(idsFromCollection(dao.fetchRecordCollection("G", 1, getMarcXMerger())), "B:870970", "F:1", "G:870970");

        collectionIs(idsFromCollection(dao.fetchRecordCollection("H", 1, getMarcXMerger())), "B:870970", "F:1", "H:1");

        collectionIs(idsFromCollection(dao.fetchRecordCollection("H", 2, getMarcXMerger())), "B:2", "F:870970", "H:870970");

        connection.commit();

    }

    @Test
    public void testFetchRecordCollectionNoCommonLibrary() throws SQLException, RawRepoException, MarcXMergerException {
        setupData(0, "B:1", "C:1", "D:1", "E:1", "F:1", "G:1", "H:1");
        setupRelations("C:1,B:1", "D:1,C:1", "E:1,C:1", "F:1,B:1", "G:1,F:1", "H:1,F:1");
        final RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);
        collectionIs(idsFromCollection(dao.fetchRecordCollection("D", 1, getMarcXMerger())), "B:1", "C:1", "D:1");
    }

    @Test
    public void testFetchRecordCollectionArticle() throws SQLException, RawRepoException, MarcXMergerException {
        setupData(800000, "A:870970", "B:870971", "B:191919");
        setupRelations("B:870971,A:870970", "B:191919,B:870971");
        final RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);
        collectionIs(idsFromCollection(dao.fetchRecordCollection("B", 870971, getMarcXMerger())), "A:870970", "B:870971");
        collectionIs(idsFromCollection(dao.fetchRecordCollection("B", 191919, getMarcXMerger())), "A:870970", "B:191919");
    }

    @Test
    public void testFetchRecordCollectionLittolk() throws SQLException, RawRepoException, MarcXMergerException {
        setupData(800000, "A:870970", "B:870974", "B:191919");
        setupRelations("B:870974,A:870970", "B:191919,B:870974");
        final RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);
        collectionIs(idsFromCollection(dao.fetchRecordCollection("B", 870974, getMarcXMerger())), "A:870970", "B:870974");
        collectionIs(idsFromCollection(dao.fetchRecordCollection("B", 191919, getMarcXMerger())), "A:870970", "B:191919");
    }

    @Test
    public void testFetchRecordCollectionMatvurd() throws SQLException, RawRepoException, MarcXMergerException {
        setupData(800000, "A:870970", "B:870976", "B:191919", "C:870970");
        setupRelations("B:870976,A:870970", "B:870976,C:870970", "B:191919,B:870976");
        final RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);
        collectionIs(idsFromCollection(dao.fetchRecordCollection("B", 870976, getMarcXMerger())), "A:870970", "B:870976", "C:870970");
        collectionIs(idsFromCollection(dao.fetchRecordCollection("B", 191919, getMarcXMerger())), "A:870970", "B:191919", "C:870970");
    }

    @Test
    public void testFetchRecordCollectionAuthority() throws SQLException, RawRepoException, MarcXMergerException {
        setupData(800000, "A:870979", "A:191919", "B:870979", "C:870970");
        setupRelations("C:870970,A:870979", "C:870970,B:870979", "A:191919,A:870979");
        final RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);
        collectionIs(idsFromCollection(dao.fetchRecordCollection("C", 870970, getMarcXMerger())), "C:870970", "A:870979", "B:870979");
        collectionIs(idsFromCollection(dao.fetchRecordCollection("A", 191919, getMarcXMerger())), "A:191919");
    }

    @Test
    public void testDeleteRecord() throws SQLException, RawRepoException {
        setupData(100000, "A:870970", "B:870970");
        final RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);

        //Ensure record is in base
        Record recA = dao.fetchRecord("A", 870970);
        assertFalse(recA.isOriginal());
        assertFalse(recA.isDeleted());
        dao.saveRecord(recA);

        recA = dao.fetchRecord("A", 870970);
        recA.setDeleted(true);
        dao.saveRecord(recA);

        assertTrue(dao.recordExistsMaybeDeleted("A", 870970));
        assertFalse(dao.recordExists("A", 870970));

        recA = dao.fetchRecord("A", 870970);
        assertFalse(recA.isOriginal());
        assertTrue(recA.isDeleted());
        recA.setDeleted(false);
        dao.saveRecord(recA);

        recA = dao.fetchRecord("A", 870970);
        assertFalse(recA.isOriginal());
        assertFalse(recA.isDeleted());

        connection.commit();
    }

        /*
                    B 870970 1 2
                  /   \
                C       F
               /  \    /  \
             D     E G      H
     */

    @Test
    public void testQueueEntityWithout() throws SQLException, RawRepoException {
        setupData(100000, "A:870970");
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);
        dao.changedRecord("test", recordIdFromString("A:870970"));
        collectionIs(getQueue(),
                "A:870970:changed",
                "A:870970:leaf");
        connection.commit();
    }

    @Test
    public void testQueueEntityWith() throws SQLException, RawRepoException {
        setupData(100000, "A:870970", "A:1", "A:2");
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
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
        setupData(100000, "A:870970", "A:1", "A:2");
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);
        dao.changedRecord("test", recordIdFromString("A:1"));
        collectionIs(getQueue(),
                "A:1:changed",
                "A:1:leaf");
        connection.commit();
    }

    @Test
    public void testQueueSectionWithComplexLocal() throws SQLException, RawRepoException {
        setupData(100000, "B:870970", "B:1", // HEAD
                "C:870970", // SECTION
                "D:870970", // BIND
                "E:870970", "E:2", // BIND
                "F:870970", "F:2", // SECTION
                "G:870970", "G:1", // BIND
                "H:870970");// BIND
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
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
        setupData(100000, "B:870970", // HEAD
                "C:870970", "C:1", // SECTION
                "D:870970", // BIND
                "E:870970", "E:2", // BIND
                "F:870970", "F:2", // SECTION
                "G:870970", "G:1", // BIND
                "H:870970");// BIND
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
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
    public void testQueueHeadWithComplexLocalHead() throws SQLException, RawRepoException {
        setupData(100000, "B:870970", "B:1", "B:2", // HEAD
                "C:870970", "C:1", "C:2",// SECTION
                "D:870970", "D:1", "D:2", // BIND
                "E:870970", "E:1", "E:2",// BIND
                "F:870970", "F:1", "F:2", // SECTION
                "G:870970", "G:1", "G:2", // BIND
                "H:870970", "H:1", "H:2");// BIND
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);
        dao.changedRecord("test", recordIdFromString("B:1"));
        collectionIs(getQueue(),
                "B:1:changed", "B:1:node",
                "C:1:node",
                "D:1:leaf",
                "E:1:leaf",
                "F:1:node",
                "G:1:leaf",
                "H:1:leaf");
        connection.commit();
    }

    @Test
    public void testQueueHeadWithComplexLocalHeadNoEnrichments() throws SQLException, RawRepoException {
        setupData(100000, "B:870970", "B:1", "B:2", // HEAD
                "C:870970", "C:2",// SECTION
                "D:870970", "D:2", // BIND
                "E:870970", "E:2",// BIND
                "F:870970", "F:2", // SECTION
                "G:870970", "G:2", // BIND
                "H:870970", "H:2");// BIND
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);
        dao.changedRecord("test", recordIdFromString("B:1"));
        collectionIs(getQueue(),
                "B:1:changed", "B:1:node",
                "C:1:node",
                "D:1:leaf",
                "E:1:leaf",
                "F:1:node",
                "G:1:leaf",
                "H:1:leaf");
        connection.commit();
    }

    @Test
    public void testQueueHeadWithComplexLocalSection() throws SQLException, RawRepoException {
        setupData(100000, "B:870970", "B:1", "B:2", // HEAD
                "C:870970", "C:1", "C:2",// SECTION
                "D:870970", "D:1", "D:2", // BIND
                "E:870970", "E:1", "E:2",// BIND
                "F:870970", "F:1", "F:2", // SECTION
                "G:870970", "G:1", "G:2", // BIND
                "H:870970", "H:1", "H:2");// BIND
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);
        dao.changedRecord("test", recordIdFromString("C:1"));
        collectionIs(getQueue(),
                "C:1:changed", "C:1:node",
                "D:1:leaf",
                "E:1:leaf");
        connection.commit();
    }

    @Test
    public void testQueueHeadWithComplexLocalVolume() throws SQLException, RawRepoException {
        setupData(100000, "B:870970", "B:1", "B:2", // HEAD
                "C:870970", "C:1", "C:2",// SECTION
                "D:870970", "D:1", "D:2", // BIND
                "E:870970", "E:1", "E:2",// BIND
                "F:870970", "F:1", "F:2", // SECTION
                "G:870970", "G:1", "G:2", // BIND
                "H:870970", "H:1", "H:2");// BIND
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);
        dao.changedRecord("test", recordIdFromString("D:1"));
        collectionIs(getQueue(),
                "D:1:changed", "D:1:leaf");
        connection.commit();
    }

    @Test
    public void testQueueHeadWithComplexCommonVolume() throws SQLException, RawRepoException {
        setupData(100000, "B:870970", "B:1", "B:2", // HEAD
                "C:870970", "C:1", "C:2",// SECTION
                "D:870970", "D:1", "D:2", // BIND
                "E:870970", "E:1", "E:2",// BIND
                "F:870970", "F:1", "F:2", // SECTION
                "G:870970", "G:1", "G:2", // BIND
                "H:870970", "H:1", "H:2");// BIND
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);
        dao.changedRecord("test", recordIdFromString("D:870970"));
        collectionIs(getQueue(),
                "D:870970:changed", "D:870970:leaf",
                "D:1:leaf", "D:2:leaf");
        connection.commit();
    }

    @Test
    public void testQueueNotUsingCommon() throws SQLException, RawRepoException {
        setupData(100000, "B:870970", // HEAD
                "C:870970", "C:1", // SECTION
                "D:870970", // BIND
                "E:870970", "E:2", // BIND
                "F:870970", "F:2", "F:999999", // SECTION
                "G:870970", "G:1", // BIND
                "H:870970");// BIND
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);
        dao.changedRecord("test", recordIdFromString("F:999999"));
        logger.info("getQueue() = " + getQueue());
        collectionIs(getQueue(),
                "F:999999:changed", "F:999999:leaf");
        connection.commit();
    }

    @Test
    public void testEnqueueWithChainedSiblings() throws SQLException, RawRepoException {
        setupData(100000, "H:870970,1",
                "S1:870970,2,3",
                "B11:870970",
                "B12:870970,4,9" // 9 is local record not sibling
        );
        setupRelations(
                "H:1,H:870970",
                "S1:870970,H:870970", "S1:2,S1:870970", "S1:3,S1:2",
                "B11:870970,S1:870970",
                "B12:870970,S1:870970", "B12:4,B12:870970");
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
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
    public void enqueueUpdatePriority() throws SQLException, RawRepoException {
        setupData(100000, "S1:870970:changed");
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);

        clearQueue();
        dao.changedRecord("test", new RecordId("S1", 870970), 1000);
        final Collection<String> queueBeforeRaise = getQueue();
        assertThat("Queue size before", queueBeforeRaise.size(), is(2));
        dao.changedRecord("test", new RecordId("S1", 870970), 500);
        final Collection<String> queueAfterRaise = getQueue();
        assertThat("Queue size after", queueAfterRaise.size(), is(2));
        final QueueJob queueJob = dao.dequeue("changed");
        assertThat(queueJob.priority, is(500));
    }

    @Test
    public void enqueueKeepPriority() throws SQLException, RawRepoException {
        setupData(100000, "S1:870970:changed");
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);

        clearQueue();
        dao.changedRecord("test", new RecordId("S1", 870970), 500);
        final Collection<String> queueBeforeRaise = getQueue();
        assertThat("Queue size before", queueBeforeRaise.size(), is(2));
        dao.changedRecord("test", new RecordId("S1", 870970), 1000);
        final Collection<String> queueAfterRaise = getQueue();
        assertThat("Queue size after", queueAfterRaise.size(), is(2));
        final QueueJob queueJob = dao.dequeue("changed");
        assertThat("Queue job priority", queueJob.priority, is(500));
    }

    @Test
    public void testCheckProvider() throws RawRepoException {
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();

        // The test database is initialized with provider 'test'
        assertTrue(dao.checkProvider("test"));
        assertFalse(dao.checkProvider("not-found"));
    }

    @Test
    public void testDequeue() throws SQLException, RawRepoException {
        setupData(100000);
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);

        dao.enqueue(new RecordId("A", 1), "test", true, true);
        connection.commit();
        collectionIs(getQueueState(),
                "A:1:changed:1", "A:1:leaf:1");

        QueueJob job = dao.dequeue("changed");
        assertThat(job, notNullValue());
    }

    @Test
    public void testDequeuePriority() throws SQLException, RawRepoException {
        setupData(100000);
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);

        // Lower number = faster dequeuing
        // No priority defaults to 1000
        dao.enqueue(new RecordId("RECORD_0", 870970), "test", true, true);
        dao.enqueue(new RecordId("RECORD_1", 870970), "test", true, true, 1000);
        dao.enqueue(new RecordId("RECORD_2", 870970), "test", true, true, 10);
        dao.enqueue(new RecordId("RECORD_3", 870970), "test", true, true, 1000);
        dao.enqueue(new RecordId("RECORD_4", 870970), "test", true, true, 5);
        dao.enqueue(new RecordId("RECORD_5", 870970), "test", true, true, 1000);
        dao.enqueue(new RecordId("RECORD_6", 870970), "test", true, true);
        dao.enqueue(new RecordId("RECORD_7", 870970), "test", true, true, 1000);
        dao.enqueue(new RecordId("RECORD_8", 870970), "test", true, true, 10);
        dao.enqueue(new RecordId("RECORD_9", 870970), "test", true, true, 42);

        connection.commit();

        collectionIs(getQueueState(),
                "RECORD_0:870970:changed:1", "RECORD_0:870970:leaf:1",
                "RECORD_1:870970:changed:1", "RECORD_1:870970:leaf:1",
                "RECORD_2:870970:changed:1", "RECORD_2:870970:leaf:1",
                "RECORD_3:870970:changed:1", "RECORD_3:870970:leaf:1",
                "RECORD_4:870970:changed:1", "RECORD_4:870970:leaf:1",
                "RECORD_5:870970:changed:1", "RECORD_5:870970:leaf:1",
                "RECORD_6:870970:changed:1", "RECORD_6:870970:leaf:1",
                "RECORD_7:870970:changed:1", "RECORD_7:870970:leaf:1",
                "RECORD_8:870970:changed:1", "RECORD_8:870970:leaf:1",
                "RECORD_9:870970:changed:1", "RECORD_9:870970:leaf:1");

        assertEquals("RECORD_4", dao.dequeue("changed").job.getBibliographicRecordId());
        assertEquals("RECORD_2", dao.dequeue("changed").job.getBibliographicRecordId());
        assertEquals("RECORD_8", dao.dequeue("changed").job.getBibliographicRecordId());
        assertEquals("RECORD_9", dao.dequeue("changed").job.getBibliographicRecordId());
        assertEquals("RECORD_0", dao.dequeue("changed").job.getBibliographicRecordId());
        assertEquals("RECORD_1", dao.dequeue("changed").job.getBibliographicRecordId());
        assertEquals("RECORD_3", dao.dequeue("changed").job.getBibliographicRecordId());
        assertEquals("RECORD_5", dao.dequeue("changed").job.getBibliographicRecordId());
        assertEquals("RECORD_6", dao.dequeue("changed").job.getBibliographicRecordId());
        assertEquals("RECORD_7", dao.dequeue("changed").job.getBibliographicRecordId());
    }

    @Test
    public void testQueueFail() throws SQLException, RawRepoException {
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);

        try (PreparedStatement stmt = connection.prepareStatement("SELECT COUNT(*) FROM jobdiag")) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (!resultSet.next() || resultSet.getInt(1) != 0) {
                    fail("jobdiag is not empty before test");
                }
            }
        }

        QueueJob queueJob = new QueueJob("abcdefgh", 123456, "node", new Timestamp(0), 1000);
        dao.queueFail(queueJob, "What!");

        try (PreparedStatement stmt = connection.prepareStatement("SELECT COUNT(*) FROM jobdiag")) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (!resultSet.next() || resultSet.getInt(1) != 1) {
                    fail("jobdiag is not set after test");
                }
            }
        }
    }

    @Test
    public void testDequeueBulk() throws SQLException, RawRepoException {
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);
        for (int i = 0; i < 10; i++) {
            dao.enqueue(new RecordId("rec" + i, 123456), "test", false, false);
        }
        connection.commit();
        connection.setAutoCommit(false);

        collectionIs(getQueueState(),
                "rec0:123456:node:1",
                "rec1:123456:node:1",
                "rec2:123456:node:1",
                "rec3:123456:node:1",
                "rec4:123456:node:1",
                "rec5:123456:node:1",
                "rec6:123456:node:1",
                "rec7:123456:node:1",
                "rec8:123456:node:1",
                "rec9:123456:node:1");

        dao.dequeue("node", 4);

        collectionIs(getQueueState(),
                "rec4:123456:node:1",
                "rec5:123456:node:1",
                "rec6:123456:node:1",
                "rec7:123456:node:1",
                "rec8:123456:node:1",
                "rec9:123456:node:1");
        connection.rollback();
        collectionIs(getQueueState(),
                "rec0:123456:node:1",
                "rec1:123456:node:1",
                "rec2:123456:node:1",
                "rec3:123456:node:1",
                "rec4:123456:node:1",
                "rec5:123456:node:1",
                "rec6:123456:node:1",
                "rec7:123456:node:1",
                "rec8:123456:node:1",
                "rec9:123456:node:1");
        connection.setAutoCommit(false);

        dao.dequeue("node", 4);

        collectionIs(getQueueState(),
                "rec4:123456:node:1",
                "rec5:123456:node:1",
                "rec6:123456:node:1",
                "rec7:123456:node:1",
                "rec8:123456:node:1",
                "rec9:123456:node:1");
        connection.commit();
        collectionIs(getQueueState(),
                "rec4:123456:node:1",
                "rec5:123456:node:1",
                "rec6:123456:node:1",
                "rec7:123456:node:1",
                "rec8:123456:node:1",
                "rec9:123456:node:1");
    }

    @Test
    public void testArchive() throws SQLException, RawRepoException {
        setupData(100000);
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);

        Record record1 = dao.fetchRecord("1 234 567 8", 123456);
        record1.setContent("HELLO".getBytes());
        record1.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
        dao.saveRecord(record1);
        connection.commit();
        connection.setAutoCommit(false);
        Record record2 = dao.fetchRecord("1 234 567 8", 123456);
        record2.setContent("HELLO AGAIN".getBytes());
        record2.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
        dao.saveRecord(record2);

        ResultSet resultSet = connection.prepareStatement("SELECT COUNT(*) FROM records_archive").executeQuery();
        resultSet.next();
        assertThat(resultSet.getInt(1), greaterThan(0));
    }


    @Test
    public void testGetAllAgencies() throws Exception {
        setupData(100000, "A:870970,101-deleted,102", "B:870970-deleted,200");
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);

        assertThat("lookup A", dao.allAgenciesForBibliographicRecordId("A"), containsInAnyOrder(870970, 101, 102));
        assertThat("lookup B", dao.allAgenciesForBibliographicRecordId("B"), containsInAnyOrder(870970, 200));
        assertThat("lookup A skip Deleted", dao.allAgenciesForBibliographicRecordIdSkipDeleted("A"), containsInAnyOrder(870970, 102));
        assertThat("lookup B skip Deleted", dao.allAgenciesForBibliographicRecordIdSkipDeleted("B"), containsInAnyOrder(200));
    }

    @Test
    public void testGetMimeTypes() throws Exception {
        setupData(100000, "X:000111", "A:870970", "B:870971", "C:870979", "D:898989", "E:870974", "F:870976");
        RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new MyRelationHints()).build();
        connection.setAutoCommit(false);

        assertEquals(MarcXChangeMimeType.MARCXCHANGE, dao.getMimeTypeOfSafe("A", 870970));
        assertEquals(MarcXChangeMimeType.ARTICLE, dao.getMimeTypeOfSafe("B", 870971));
        assertEquals(MarcXChangeMimeType.AUTHORITY, dao.getMimeTypeOfSafe("C", 870979));
        assertEquals(MarcXChangeMimeType.UNKNOWN, dao.getMimeTypeOfSafe("D", 898989));
        assertEquals(MarcXChangeMimeType.UNKNOWN, dao.getMimeTypeOfSafe("E", 898989));
        assertEquals(MarcXChangeMimeType.LITANALYSIS, dao.getMimeTypeOfSafe("E", 870974));
        assertEquals(MarcXChangeMimeType.MATVURD, dao.getMimeTypeOfSafe("F", 870976));
        assertEquals(MarcXChangeMimeType.ENRICHMENT, dao.getMimeTypeOfSafe("X", 111));

        try {
            logger.info("Nothing should be written here " + dao.getMimeTypeOf("E", 898989));
        } catch (RawRepoExceptionRecordNotFound t) {
            assertEquals("Trying to find mimetype", t.getMessage());
        }
    }

    //  _   _      _                   _____                 _   _
    // | | | | ___| |_ __   ___ _ __  |  ___|   _ _ __   ___| |_(_) ___  _ __  ___
    // | |_| |/ _ \ | '_ \ / _ \ '__| | |_ | | | | '_ \ / __| __| |/ _ \| '_ \/ __|
    // |  _  |  __/ | |_) |  __/ |    |  _|| |_| | | | | (__| |_| | (_) | | | \__ \
    // |_| |_|\___|_| .__/ \___|_|    |_|   \__,_|_| |_|\___|\__|_|\___/|_| |_|___/
    //              |_|
    private void resetDatabase() throws SQLException {
        postgres.clearTables("relations", "records", "records_archive", "queue", "queuerules", "queueworkers", "jobdiag");

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

    /**
     * Create Test Data
     *
     * @param maxEnrichmentLibrary Record with AgencyLower records Get mimeType ENRICHMENT
     * @param ids                  Set of Ids of the form [BibliographicId:Agency,Agency] The Agency Can have a -delete attach for creating records with deleted='t'
     * @throws RawRepoException on Dao Errors
     * @throws SQLException     Errors from Commits
     */
    private void setupData(int maxEnrichmentLibrary, String... ids) throws RawRepoException, SQLException {
        connection.setAutoCommit(false);
        RawRepoDAO dao = RawRepoDAO.builder(connection).build();
        Map<String, Set<RecordId>> idMap = new HashMap<>();
        for (String id : ids) {
            idMap.put(id, new HashSet<>());
        }
        Set<String> keys = idMap.keySet();

        for (String id : keys) {
            String[] split1 = id.split(":");
            String[] split2 = split1[1].split(",");
            for (String lib : split2) {
                boolean isDeleted = false;
                if (lib.endsWith("-deleted")) {
                    isDeleted = true;
                    lib = lib.replace("-deleted", "");
                }
                RecordId recordId = new RecordId(split1[0], Integer.parseInt(lib));
                Record record = dao.fetchRecord(recordId.getBibliographicRecordId(), recordId.getAgencyId());
                String mimeType;
                if (recordId.getAgencyId() < maxEnrichmentLibrary) {
                    mimeType = MarcXChangeMimeType.ENRICHMENT;
                } else if (recordId.getAgencyId() == 870979) {
                    mimeType = MarcXChangeMimeType.AUTHORITY;
                } else if (recordId.getAgencyId() == 870971) {
                    mimeType = MarcXChangeMimeType.ARTICLE;
                } else if (recordId.getAgencyId() == 870970) {
                    mimeType = MarcXChangeMimeType.MARCXCHANGE;
                } else if (recordId.getAgencyId() == 870974) {
                    mimeType = MarcXChangeMimeType.LITANALYSIS;
                } else if (recordId.getAgencyId() == 870976) {
                    mimeType = MarcXChangeMimeType.MATVURD;
                } else {
                    mimeType = MarcXChangeMimeType.UNKNOWN;
                }
                record.setMimeType(mimeType);
                record.setContent(id.getBytes());
                record.setDeleted(isDeleted);
                dao.saveRecord(record);
            }
        }
        setupRelations(RELATIONS);
        connection.commit();
    }

    private void setupRelations(String... relations) throws NumberFormatException, RawRepoException, SQLException {
        connection.setAutoCommit(false);
        RawRepoDAO dao = RawRepoDAO.builder(connection).build();
        for (String relation : relations) {
            String[] list = relation.split(",", 2);
            RecordId from = recordIdFromString(list[0]);
            RecordId to = recordIdFromString(list[1]);
            if (dao.recordExists(from.getBibliographicRecordId(), from.getAgencyId()) &&
                    dao.recordExists(to.getBibliographicRecordId(), to.getAgencyId())) {
                Set<RecordId> relationsFrom = dao.getRelationsFrom(from);
                relationsFrom.add(to);
                dao.setRelationsFrom(from, relationsFrom);
            }
        }
        connection.commit();
    }

    private void clearQueue() throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("DELETE FROM QUEUE");
        stmt.execute();
    }

    private Collection<String> getQueue() throws SQLException {
        Set<String> result = new HashSet<>();
        PreparedStatement stmt = connection.prepareStatement("SELECT bibliographicrecordid, agencyid, worker FROM QUEUE");
        if (stmt.execute()) {
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                result.add(resultSet.getString(1) + ":" + resultSet.getInt(2) + ":" + resultSet.getString(3));
            }
        }
        return result;
    }

    private Collection<String> getQueueState() throws SQLException {
        Set<String> result = new HashSet<>();
        PreparedStatement stmt = connection.prepareStatement("SELECT bibliographicrecordid, agencyid, worker, COUNT(queued) FROM QUEUE GROUP BY bibliographicrecordid, agencyid, worker");
        if (stmt.execute()) {
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                result.add(resultSet.getString(1) + ":" + resultSet.getInt(2) + ":" + resultSet.getString(3) + ":" + resultSet.getInt(4));
            }
        }
        return result;
    }

    private Collection<String> idsFromCollection(Map<String, Record> records) {
        Collection<String> collection = new HashSet<>();
        for (Record record : records.values()) {
            collection.add(record.getId().getBibliographicRecordId() + ":" + record.getId().getAgencyId());
        }
        return collection;
    }

    /**
     * Raise an (descriptive) exception if a collection of strings doesn't match
     * supplied list
     *
     * @param col   collection
     * @param elems string elements collection should consist of
     */
    private static void collectionIs(Collection<String> col, String... elems) {
        HashSet<String> missing = new HashSet<>();
        Collections.addAll(missing, elems);
        HashSet<String> extra = new HashSet<>(col);
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
     * @throws NumberFormatException Id was not an integer
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

    private MarcXMerger getMarcXMerger() throws MarcXMergerException {

        if (merger == null) {
            merger = new MarcXMerger() {
                @Override
                public byte[] merge(byte[] common, byte[] local, boolean isFinal) {
                    return local;
                }
            };
        }

        return merger;
    }


    private static class MyRelationHints extends RelationHintsVipCore {

        MyRelationHints() {
            super(null);
        }

        public List<Integer> get(int agencyId) {
            if (agencyId == 999999) {
                return Collections.singletonList(999999);
            }
            return Arrays.asList(870970, 870971, 870979);
        }

        public boolean usesCommonAgency(int agencyId) {
            return agencyId != 999999;
        }
    }

}
