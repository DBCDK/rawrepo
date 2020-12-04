/*
 * dbc-rawrepo-rollback
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-rollback.
 *
 * dbc-rawrepo-rollback is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-rollback is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-rollback.  If not, see <http://www.gnu.org/licenses/>.
 */

package dk.dbc.rawrepo.rollback;

import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RecordMetaDataHistory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class RollBackIT {

    private static final int AGENCY_ID = 100000;
    private static final String BIB_RECORD_ID_1 = "a bcd efg h";
    private static final String BIB_RECORD_ID_2 = "q wer tyu i";

    private final static Instant NOW;
    private final static Instant DAY_1;
    private final static Instant DAY_3;
    private final static Instant DAY_4;
    private final static Instant DAY_5;

    static {
        NOW = Instant.now();

        DAY_1 = NOW.minus(7, ChronoUnit.DAYS);
        DAY_3 = NOW.minus(5, ChronoUnit.DAYS);
        DAY_4 = NOW.minus(4, ChronoUnit.DAYS);
        DAY_5 = NOW.minus(3, ChronoUnit.DAYS);
    }

    private Connection connection;

    @BeforeEach
    public void setup() throws SQLException {
        String jdbc;
        String port = System.getProperty("postgresql.port");
        jdbc = "jdbc:postgresql://localhost:" + port + "/rawrepo";
        Properties properties = new Properties();

        connection = DriverManager.getConnection(jdbc, properties);
        connection.prepareStatement("SET log_statement = 'all';").execute();
        resetDatabase();
    }

    @AfterEach
    public void teardown() throws SQLException {
        connection.close();
    }

    private void resetDatabase() throws SQLException {
        connection.prepareStatement("DELETE FROM relations").execute();
        connection.prepareStatement("DELETE FROM records").execute();
        connection.prepareStatement("DELETE FROM records_archive").execute();
        connection.prepareStatement("DELETE FROM queue").execute();
        connection.prepareStatement("DELETE FROM queuerules").execute();
        connection.prepareStatement("DELETE FROM queueworkers").execute();
        connection.prepareStatement("DELETE FROM jobdiag").execute();
    }

    @Test
    public void testHistoricRecord() throws SQLException, RawRepoException {

        RawRepoDAO dao = RawRepoDAO.builder(connection).build();
        connection.setAutoCommit(false);
        Record record;
        record = dao.fetchRecord(BIB_RECORD_ID_1, AGENCY_ID);
        record.setContent("Version 1".getBytes());
        record.setMimeType("text/plain");
        record.setDeleted(false);
        record.setModified(DAY_1);
        dao.saveRecord(record);
        connection.commit();

        connection.setAutoCommit(false);
        record = dao.fetchRecord(BIB_RECORD_ID_1, AGENCY_ID);
        record.setContent("Version 2".getBytes());
        record.setMimeType("text/not-so-plain");
        record.setDeleted(false);
        record.setModified(DAY_3);
        dao.saveRecord(record);
        connection.commit();

        connection.setAutoCommit(false);
        record = dao.fetchRecord(BIB_RECORD_ID_1, AGENCY_ID);
        record.setContent("Version 3".getBytes());
        record.setMimeType("text/really-plain");
        record.setDeleted(true);
        record.setModified(DAY_5);
        dao.saveRecord(record);
        connection.commit();

        List<RecordMetaDataHistory> recordHistory = dao.getRecordHistory(BIB_RECORD_ID_1, AGENCY_ID);
        assertThat(recordHistory.toString(), recordHistory.size(), is(3));

        record = dao.fetchRecord(BIB_RECORD_ID_1, AGENCY_ID);
        assertEquals("Version 3", new String(record.getContent(), StandardCharsets.UTF_8));

        RollBack.rollbackRecord(connection, new RecordId(BIB_RECORD_ID_1, AGENCY_ID), DAY_4, DateMatch.Match.Before, RollBack.State.Rollback, null);
        connection.commit();

        record = dao.fetchRecord(BIB_RECORD_ID_1, AGENCY_ID);
        assertEquals("Version 2", new String(record.getContent(), StandardCharsets.UTF_8));

        RollBack.rollbackRecord(connection, new RecordId(BIB_RECORD_ID_1, AGENCY_ID), DAY_4, DateMatch.Match.After, RollBack.State.Rollback, null);
        connection.commit();

        record = dao.fetchRecord(BIB_RECORD_ID_1, AGENCY_ID);
        assertEquals("Version 3", new String(record.getContent(), StandardCharsets.UTF_8));
    }

    @Test
    public void testBulkAgency_with2Records() throws SQLException, RawRepoException {
        RawRepoDAO dao = RawRepoDAO.builder(connection).build();
        connection.setAutoCommit(false);
        Record record;
        record = dao.fetchRecord(BIB_RECORD_ID_1, AGENCY_ID);
        record.setContent("Rec 1 Version 1".getBytes());
        record.setMimeType("text/plain");
        record.setDeleted(false);
        record.setModified(DAY_1);
        dao.saveRecord(record);
        connection.commit();

        connection.setAutoCommit(false);
        record = dao.fetchRecord(BIB_RECORD_ID_1, AGENCY_ID);
        record.setContent("Rec 1 Version 2".getBytes());
        record.setMimeType("text/not-so-plain");
        record.setDeleted(false);
        record.setModified(DAY_4);
        dao.saveRecord(record);
        connection.commit();

        connection.setAutoCommit(false);
        record = dao.fetchRecord(BIB_RECORD_ID_2, AGENCY_ID);
        record.setContent("Rec 2 Version 1".getBytes());
        record.setMimeType("text/really-plain");
        record.setDeleted(true);
        record.setModified(DAY_3);
        dao.saveRecord(record);
        connection.commit();

        connection.setAutoCommit(false);
        record = dao.fetchRecord(BIB_RECORD_ID_2, AGENCY_ID);
        record.setContent("Rec 2 Version 2".getBytes());
        record.setMimeType("text/really-plain");
        record.setDeleted(true);
        record.setModified(DAY_5);
        dao.saveRecord(record);
        connection.commit();

        List<RecordMetaDataHistory> recordHistory = dao.getRecordHistory(BIB_RECORD_ID_1, AGENCY_ID);
        assertThat(recordHistory.toString(), recordHistory.size(), is(2));
        recordHistory = dao.getRecordHistory(BIB_RECORD_ID_2, AGENCY_ID);
        assertThat(recordHistory.toString(), recordHistory.size(), is(2));

        record = dao.fetchRecord(BIB_RECORD_ID_1, AGENCY_ID);
        assertEquals("Rec 1 Version 2", new String(record.getContent(), StandardCharsets.UTF_8));

        record = dao.fetchRecord(BIB_RECORD_ID_2, AGENCY_ID);
        assertEquals("Rec 2 Version 2", new String(record.getContent(), StandardCharsets.UTF_8));

        RollBack.rollbackAgency(connection, AGENCY_ID, DAY_3, DateMatch.Match.Before, RollBack.State.Rollback, null);
        connection.commit();

        record = dao.fetchRecord(BIB_RECORD_ID_1, AGENCY_ID);
        assertThat("First record is rolled back", new String(record.getContent(), StandardCharsets.UTF_8), is("Rec 1 Version 1"));

        record = dao.fetchRecord(BIB_RECORD_ID_2, AGENCY_ID);
        assertThat("Second record is not rolled back", new String(record.getContent(), StandardCharsets.UTF_8), is("Rec 2 Version 2"));

        RollBack.rollbackAgency(connection, AGENCY_ID, DAY_4, DateMatch.Match.Before, RollBack.State.Rollback, null);
        connection.commit();

        record = dao.fetchRecord(BIB_RECORD_ID_1, AGENCY_ID);
        assertThat("First record is rolled back", new String(record.getContent(), StandardCharsets.UTF_8), is("Rec 1 Version 1"));

        record = dao.fetchRecord(BIB_RECORD_ID_2, AGENCY_ID);
        assertThat("Second record is rolled back", new String(record.getContent(), StandardCharsets.UTF_8), is("Rec 2 Version 1"));
    }

}
