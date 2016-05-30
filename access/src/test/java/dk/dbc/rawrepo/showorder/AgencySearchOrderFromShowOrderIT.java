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
package dk.dbc.rawrepo.showorder;

import dk.dbc.commons.testutils.postgres.connection.PostgresITConnection;
import dk.dbc.gracefulcache.CacheTimeoutException;
import dk.dbc.gracefulcache.CacheValueException;
import dk.dbc.rawrepo.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
public class AgencySearchOrderFromShowOrderIT {

    private Connection connection;
    private PostgresITConnection postgres;

    @Before
    public void setup() throws SQLException, ClassNotFoundException {
        postgres = new PostgresITConnection("rawrepo");
        connection = postgres.getConnection();
        resetDatabase();
    }

    @After
    public void teardown() throws SQLException {
        connection.close();
    }

    @Test
    public void testReadWriteRecord() throws SQLException, ClassNotFoundException, RawRepoException, IOException, MarcXMergerException {
        RawRepoDAO dao = RawRepoDAO.builder(connection)
                   .searchOrder(new AgencySearchOrderFallback("870970"))
                   .relationHints(new MyRelationHints())
                   .build();
        connection.setAutoCommit(false);

        Record rec;
        Set<RecordId> set;
        rec = dao.fetchRecord("87654321", 870970);
        rec.setContent(read("870970-87654321.xml"));
        rec.setDeleted(false);
        rec.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
        dao.saveRecord(rec);

        rec = dao.fetchRecord("12345678", 870970);
        rec.setContent(read("870970-12345678.xml"));
        rec.setDeleted(false);
        rec.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
        dao.saveRecord(rec);

        set = new HashSet<>();
        set.add(new RecordId("87654321", 870970));
        dao.setRelationsFrom(rec.getId(), set);

        rec = dao.fetchRecord("87654321", 191919);
        rec.setContent(read("191919-87654321.xml"));
        rec.setDeleted(false);
        rec.setMimeType(MarcXChangeMimeType.ENRICHMENT);
        dao.saveRecord(rec);

        set = new HashSet<>();
        set.add(new RecordId("87654321", 870970));
        dao.setRelationsFrom(rec.getId(), set);

        rec = dao.fetchRecord("12345678", 191919);
        rec.setContent(read("191919-12345678.xml"));
        rec.setDeleted(false);
        rec.setMimeType(MarcXChangeMimeType.ENRICHMENT);
        dao.saveRecord(rec);

        set = new HashSet<>();
        set.add(new RecordId("12345678", 870970));
        dao.setRelationsFrom(rec.getId(), set);

        rec = dao.fetchRecord("87654321", 777777);
        rec.setContent(read("777777-87654321.xml"));
        rec.setDeleted(false);
        rec.setMimeType(MarcXChangeMimeType.ENRICHMENT);
        dao.saveRecord(rec);

        set = new HashSet<>();
        set.add(new RecordId("12345678", 870970));
        dao.setRelationsFrom(rec.getId(), set);

        connection.commit();

        connection.setAutoCommit(false);
        Map<String, Record> fetchRecordCollection = dao.fetchRecordCollection("12345678", 777777, new MarcXMerger());
        System.out.println("fetchRecordCollection = " + fetchRecordCollection);
        assertEquals("Size of collection", 2, fetchRecordCollection.size());
        assertEquals("Parent", 777777, fetchRecordCollection.get("87654321").getId().getAgencyId());
        assertEquals("Child", 870970, fetchRecordCollection.get("12345678").getId().getAgencyId());
        connection.commit();

    }

//  _   _      _                   _____                 _   _
// | | | | ___| |_ __   ___ _ __  |  ___|   _ _ __   ___| |_(_) ___  _ __  ___
// | |_| |/ _ \ | '_ \ / _ \ '__| | |_ | | | | '_ \ / __| __| |/ _ \| '_ \/ __|
// |  _  |  __/ | |_) |  __/ |    |  _|| |_| | | | | (__| |_| | (_) | | | \__ \
// |_| |_|\___|_| .__/ \___|_|    |_|   \__,_|_| |_|\___|\__|_|\___/|_| |_|___/
//              |_|
    void resetDatabase() throws SQLException {
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

    private byte[] read(String content) throws IOException {
        InputStream is = getClass().getResourceAsStream("/AgencySearchOrder/" + content);
        if (is == null) {
            throw new NoSuchFileException(content);
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        int nRead;
        byte[] data = new byte[16384];

        while (( nRead = is.read(data, 0, data.length) ) != -1) {
            buffer.write(data, 0, nRead);
        }

        buffer.flush();

        return buffer.toByteArray();
    }


    private static class MyRelationHints extends RelationHints {

        public MyRelationHints() {
        }

        @Override
        public List<Integer> get(int agencyId) throws CacheTimeoutException, CacheValueException {
            return Arrays.asList(870970);
        }

        @Override
        public boolean usesCommonAgency(int agencyId) throws RawRepoException {
            return true;
        }
    }

}
