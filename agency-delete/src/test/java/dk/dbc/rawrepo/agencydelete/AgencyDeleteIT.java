/*
 * dbc-rawrepo-agency-delete
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-agency-delete.
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
 * along with dbc-rawrepo-agency-delete.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.agencydelete;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import dk.dbc.commons.testutils.postgres.connection.PostgresITConnection;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.AgencySearchOrderFallback;
import dk.dbc.rawrepo.QueueJob;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.github.tomakehurst.wiremock.client.WireMock.*;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.*;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class AgencyDeleteIT {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(getPort()).withRootDirectory(getPath()));

    static int fallbackPort = (int) ( 15000.0 * Math.random() ) + 15000;

    static int getPort() {
        int port = Integer.parseInt(System.getProperty("wiremock.port", "0"));
        if (port == 0) {
            port = fallbackPort;
        }
        return port;
    }

    static String getPath() {
        return wireMockConfig().filesRoot().child("RelationHintsOpenAgency").getPath();
    }

    private String jdbcUrl;
    private Connection connection;
    private PostgresITConnection postgresITConnection;

    public AgencyDeleteIT() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws Exception {
        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:libraryRulesRequest/ns1:agencyId[.='777777']")
                        .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_libraryRules_777777.xml")));
        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:showOrderRequest/ns1:agencyId[.='777777']")
                        .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_showOrder_777777.xml")));

        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:libraryRulesRequest/ns1:agencyId[.='888888']")
                        .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_libraryRules_888888.xml")));
        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:showOrderRequest/ns1:agencyId[.='888888']")
                        .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_showOrder_888888.xml")));

        postgresITConnection = new PostgresITConnection("rawrepo");
        connection = postgresITConnection.getConnection();
        jdbcUrl = postgresITConnection.getUrl();
        setupRecords();
    }

    @After
    public void tearDown() {
        try {
            if (!connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testBasics() throws Exception {
        System.out.println("testBasics()");
        AgencyDelete agencyDelete = new AgencyDelete(jdbcUrl, 777777, "http://localhost:" + getPort() + "/openagency/");

        agencyDelete.begin();
        Set<String> ids = agencyDelete.getIds();
        testArray(ids, "Ids", "H", "S", "B", "E");

        agencyDelete.queueRecords(ids, "provider");
        System.out.println("queued records");
        agencyDelete.deleteRecords(ids);
        System.out.println("deleted records");
        agencyDelete.commit();

        RawRepoDAO dao = RawRepoDAO.builder(connection)
                   .openAgency(OpenAgencyServiceFromURL.builder().build("http://localhost:" + getPort() + "/openagency/"), null)
                   .build();

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
            RawRepoDAO dao = RawRepoDAO.builder(connection)
                       .openAgency(OpenAgencyServiceFromURL.builder().build("http://localhost:" + getPort() + "/openagency/"), null)
                       .build();
            connection.setAutoCommit(false);
            setupRecord(dao, "S", 888888, "S:870970");
            connection.commit();
        }
        AgencyDelete agencyDelete = new AgencyDelete(jdbcUrl, 888888, "http://localhost:" + getPort() + "/openagency/");
        agencyDelete.begin();
        Set<String> ids = agencyDelete.getIds();

        agencyDelete.queueRecords(ids, "provider");
        agencyDelete.deleteRecords(ids);
        agencyDelete.commit();

        countQueued("node", 1);
        countQueued("leaf", 1);

    }

    @Test
    public void textGetIds() throws Exception {
        {
            RawRepoDAO dao = RawRepoDAO.builder(connection)
                       .openAgency(OpenAgencyServiceFromURL.builder().build("http://localhost:" + getPort() + "/openagency/"), null)
                       .build();
            connection.setAutoCommit(false);
            setupRecord(dao, "H", 888888, "S:870970");
            setupRecord(dao, "E", 888888, "S:870970");
            connection.commit();
        }

        AgencyDelete agencyDelete = new AgencyDelete(jdbcUrl, 888888, "http://localhost:" + getPort() + "/openagency/");

        Set<String> nodes = agencyDelete.getIds();
        System.out.println("nodes = " + nodes);

        assertTrue("has H", nodes.contains("H"));
        assertTrue("has S", nodes.contains("S"));
        assertTrue("has B", nodes.contains("B"));
        assertTrue("has E", nodes.contains("E"));
        assertEquals("has no extras", 4, nodes.size());
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
            try (InputStream is = AgencyDeleteIT.class.getClassLoader().getResourceAsStream(res)) {
                int available = is.available();
                byte[] bytes = new byte[available];
                is.read(bytes);
                return bytes;
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void setupRecords() throws RawRepoException, SQLException, UnsupportedEncodingException {
        RawRepoDAO dao = RawRepoDAO.builder(connection).searchOrder(new AgencySearchOrderFallback("870970")).build();
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

        stmt = connection.prepareStatement("INSERT INTO queuerules(provider, worker, changed, leaf) VALUES(?, ?, 'A', ?)");
        stmt.setString(1, "provider");
        stmt.setString(2, "leaf");
        stmt.setString(3, "Y");
        stmt.execute();
        stmt.setString(1, "provider");
        stmt.setString(2, "node");
        stmt.setString(3, "N");
        stmt.execute();

        setupRecord(dao, "H", 870970);
        setupRecord(dao, "S", 870970, "H:870970");
        setupRecord(dao, "B", 870970, "S:870970");
        setupRecord(dao, "E", 870970);

        setupRecord(dao, "H", 777777, "H:870970");
        setupRecord(dao, "S", 777777, "S:870970");
        setupRecord(dao, "B", 777777, "B:870970");
        setupRecord(dao, "E", 777777);

        connection.commit();

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
