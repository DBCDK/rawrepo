/*
 * Copyright (C) 2016 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-agency-purge
 *
 * dbc-rawrepo-agency-purge is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-agency-purge is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.agencypurge;

import dk.dbc.commons.testutils.postgres.connection.PostgresITConnection;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * @author DBC {@literal <dbc.dk>}
 */
public class AgencyPurgeIT {
    private static Connection connection;
    private static String jdbcUrl;

    @BeforeEach
    public static void setUp() throws SQLException {
        final PostgresITConnection postgresITConnection = new PostgresITConnection("rawrepo");
        postgresITConnection.clearTables("queue", "relations", "records", "records_archive");
        connection = postgresITConnection.getConnection();
        jdbcUrl = postgresITConnection.getUrl();
    }

    @AfterEach
    public static void tearDown() {
        try {
            if (!connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Test of unittestObject method, of class AgencyPurge.
     */
    @Test
    public void testCountUndeleted() throws Exception {
        setupRecords("A 870970",
                "A 888888 A:870970");
        AgencyPurge agencyPurge = new AgencyPurge(jdbcUrl, 888888);
        int notDeleted = agencyPurge.countRecordsNotDeleted();
        assertEquals(1, notDeleted);
    }


    @Test
    public void testCountQueue() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("INSERT INTO queueworkers VALUES('test')");
            stmt.executeUpdate("INSERT INTO queue(bibliographicrecordid, agencyid, worker) VALUES('A', 888888, 'test')");
        }
        AgencyPurge agencyPurge = new AgencyPurge(jdbcUrl, 888888);
        int queued = agencyPurge.countQueueEntries();
        assertEquals(1, queued);
    }


    private void setupRecords(String... args) throws SQLException, RawRepoException, UnsupportedEncodingException {
        RawRepoDAO dao = RawRepoDAO.builder(connection)
                .build();
        try {
            connection.setAutoCommit(false);
            for (String arg : args) {
                String[] split = arg.split(" ");
                String[] relations = Arrays.copyOfRange(split, 2, split.length);

                setupRecord(dao, split[0], Integer.parseInt(split[1]), relations);
            }
        } finally {
            connection.commit();
        }
    }

    private void setupRecord(RawRepoDAO dao, String bibliographicRecordId, int agencyId, String... relations) throws RawRepoException, UnsupportedEncodingException {
        System.out.println("bibliographicRecordId = " + bibliographicRecordId + "; agencyId = " + agencyId);
        Record rec = dao.fetchRecord(bibliographicRecordId, agencyId);
        rec.setContent(content(bibliographicRecordId, String.valueOf(agencyId)));
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

    private static final String TMPL = new String(getResource("tmpl.xml"), StandardCharsets.UTF_8);

    private static byte[] content(String bibiolgraphicRecordId, String agencyId) {
        String title = "title of: " + bibiolgraphicRecordId + " from " + agencyId;

        return TMPL.replaceAll("@bibliographicrecordid@", bibiolgraphicRecordId)
                .replaceAll("@agencyid@", agencyId)
                .replaceAll("@title@", title).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] getResource(String res) {
        try {
            try (InputStream is = AgencyPurgeIT.class.getClassLoader().getResourceAsStream(res)) {
                int available = is.available();
                byte[] bytes = new byte[available];
                is.read(bytes);
                return bytes;
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
