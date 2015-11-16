/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo
 *
 * dbc-rawrepo is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.showorder;

import dk.dbc.rawrepo.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
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
import java.util.Properties;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class AgencySearchOrderFromShowOrderIT {

    private Connection connection;
    private String jdbc;

    @Before
    public void setup() throws SQLException, ClassNotFoundException {
        String port = System.getProperty("postgresql.port");
        jdbc = "jdbc:postgresql://localhost:" + port + "/rawrepo";
        Properties properties = new Properties();

        connection = DriverManager.getConnection(jdbc, properties);
        connection.prepareStatement("SET log_statement = 'all';").execute();
        resetDatabase();
    }

    @After
    public void teardown() throws SQLException {
        connection.close();
    }

    @Test
    public void testReadWriteRecord() throws SQLException, ClassNotFoundException, RawRepoException, IOException, MarcXMergerException {
        RawRepoDAO dao = RawRepoDAO.builder(connection)
                   .searchOrder(new AgencySearchOrderFallback("870970,191919"))
                   .build();
        connection.setAutoCommit(false);

        Record rec;
        Set<RecordId> set;
        rec = dao.fetchRecord("87654321", 191919);
        rec.setContent(read("191919-87654321.xml"));
        rec.setDeleted(false);
        rec.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
        dao.saveRecord(rec);

        rec = dao.fetchRecord("12345678", 191919);
        rec.setContent(read("191919-12345678.xml"));
        rec.setDeleted(false);
        rec.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
        dao.saveRecord(rec);

        set = new HashSet<>();
        set.add(new RecordId("87654321", 191919));
        dao.setRelationsFrom(rec.getId(), set);

        rec = dao.fetchRecord("87654321", 870970);
        rec.setContent(read("870970-87654321.xml"));
        rec.setDeleted(false);
        rec.setMimeType(MarcXChangeMimeType.ENRICHMENT);
        dao.saveRecord(rec);

        set = new HashSet<>();
        set.add(new RecordId("87654321", 191919));
        dao.setRelationsFrom(rec.getId(), set);

        rec = dao.fetchRecord("12345678", 870970);
        rec.setContent(read("870970-12345678.xml"));
        rec.setDeleted(false);
        rec.setMimeType(MarcXChangeMimeType.ENRICHMENT);
        dao.saveRecord(rec);

        set = new HashSet<>();
        set.add(new RecordId("12345678", 191919));
        dao.setRelationsFrom(rec.getId(), set);

        rec = dao.fetchRecord("87654321", 777777);
        rec.setContent(read("777777-87654321.xml"));
        rec.setDeleted(false);
        rec.setMimeType(MarcXChangeMimeType.ENRICHMENT);
        dao.saveRecord(rec);

        set = new HashSet<>();
        set.add(new RecordId("12345678", 191919));
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
        connection.prepareStatement("DELETE FROM relations").execute();
        connection.prepareStatement("DELETE FROM records").execute();
        connection.prepareStatement("DELETE FROM records_archive").execute();
        connection.prepareStatement("DELETE FROM queue").execute();
        connection.prepareStatement("DELETE FROM queuerules").execute();
        connection.prepareStatement("DELETE FROM queueworkers").execute();
        connection.prepareStatement("DELETE FROM jobdiag").execute();

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

    void setupData(int maxEnrichmentLibrary, String... ids) throws RawRepoException, SQLException {
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
                RecordId recordId = new RecordId(split1[0], Integer.parseInt(lib));
                Record record = dao.fetchRecord(recordId.getBibliographicRecordId(), recordId.getAgencyId());
                record.setMimeType(recordId.getAgencyId() < maxEnrichmentLibrary ? MarcXChangeMimeType.ENRICHMENT : MarcXChangeMimeType.MARCXCHANGE);
                record.setContent(id.getBytes());
                dao.saveRecord(record);
            }
        }
        setupRelations(RELATIONS);
        connection.commit();
    }

    void setupRelations(String... relations) throws NumberFormatException, RawRepoException, SQLException {
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

    void clearQueue() throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("DELETE FROM QUEUE");
        stmt.execute();
    }

    Collection<String> getQueue() throws SQLException {
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

    Collection<String> getQueueState() throws SQLException {
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

    public Collection<String> idsFromCollection(Map<String, Record> records) {
        Collection<String> collection = new HashSet<>();
        for (Record record : records.values()) {
            collection.add(record.getId().getBibliographicRecordId() + ":" + record.getId().getAgencyId());
        }
        return collection;
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
