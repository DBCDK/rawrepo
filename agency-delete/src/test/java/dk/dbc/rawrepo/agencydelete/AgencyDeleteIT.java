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

import com.github.tomakehurst.wiremock.WireMockServer;
import dk.dbc.commons.testutils.postgres.connection.PostgresITConnection;
import dk.dbc.httpclient.HttpClient;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.rawrepo.QueueJob;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RelationHintsVipCore;
import dk.dbc.vipcore.VipCoreConnector;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.ws.rs.client.Client;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * @author DBC {@literal <dbc.dk>}
 */
class AgencyDeleteIT {
    private static WireMockServer wireMockServer;
    private static String wireMockHost;
    private static String jdbcUrl;
    private static Connection connection;

    final static Client CLIENT = HttpClient.newClient(new ClientConfig()
            .register(new JacksonFeature()));
    private static VipCoreLibraryRulesConnector connector;

    @BeforeAll
    static void startWireMockServer() {
        wireMockServer = new WireMockServer(options().dynamicPort()
                .dynamicHttpsPort()
                .withRootDirectory(wireMockConfig().filesRoot().child("wiremock/vipcore").getPath()));
        wireMockServer.start();
        wireMockHost = "http://localhost:" + wireMockServer.port();
        configureFor("localhost", wireMockServer.port());
    }

    @BeforeAll
    static void setConnector() {
        connector = new VipCoreLibraryRulesConnector(CLIENT, wireMockHost, 0, VipCoreConnector.TimingLogLevel.INFO);
    }

    @AfterAll
    static void stopWireMockServer() {
        wireMockServer.stop();
    }

    @BeforeEach
    public void setUp() throws Exception {
        PostgresITConnection postgresITConnection = new PostgresITConnection("rawrepo");
        connection = postgresITConnection.getConnection();
        jdbcUrl = postgresITConnection.getUrl();
        setupRecords();
    }

    @AfterEach
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
    void testBasics() throws Exception {
        AgencyDelete agencyDelete = new AgencyDelete(jdbcUrl, 777777, "http://localhost:" + wireMockServer.port());

        agencyDelete.begin();
        Set<String> ids = agencyDelete.getIds();
        testArray(ids, "Ids", "H", "S", "B", "E");

        agencyDelete.queueRecords(ids, "provider");
        agencyDelete.deleteRecords(ids);
        agencyDelete.commit();

        RawRepoDAO dao = RawRepoDAO.builder(connection)
                .relationHints(new RelationHintsVipCore(connector))
                .build();

        countQueuedTotal(8);
        countQueued(777777, 8);

        countQueued("leaf", 4);
        HashSet<String> leafs = new HashSet<>();
        for (QueueJob dequeue = dao.dequeue("leaf"); dequeue != null; dequeue = dao.dequeue("leaf")) {
            leafs.add(dequeue.getJob().getBibliographicRecordId());
        }
        testArray(leafs, "leafs", "H", "S", "B", "E");

        final HashSet<String> nodes = new HashSet<>();
        countQueued("node", 4);
        for (QueueJob dequeue = dao.dequeue("node"); dequeue != null; dequeue = dao.dequeue("node")) {
            nodes.add(dequeue.getJob().getBibliographicRecordId());
        }
        testArray(nodes, "nodes", "H", "S", "B", "E");
        connection.commit();
    }

    @Test
    void testQueue() throws Exception {
        {
            RawRepoDAO dao = RawRepoDAO.builder(connection)
                    .relationHints(new RelationHintsVipCore(connector))
                    .build();
            connection.setAutoCommit(false);
            setupRecord(dao, "S", 888888, "S:870970");
            setupRecord(dao, "B", 999999, "B:870970");
            connection.commit();
        }
        AgencyDelete agencyDelete = new AgencyDelete(jdbcUrl, 888888, "http://localhost:" + wireMockServer.port());
        agencyDelete.begin();
        Set<String> ids = agencyDelete.getIds();
        testArray(ids, "nodes", "S", "B");

        agencyDelete.queueRecords(ids, "provider");
        agencyDelete.deleteRecords(ids);
        agencyDelete.commit();

        countQueuedTotal(4);
        countQueued(888888, 4);
        countQueued("node", 2);
        countQueued("leaf", 2);
    }

    @Test
    void textGetIds() throws Exception {
        {
            RawRepoDAO dao = RawRepoDAO.builder(connection)
                    .relationHints(new RelationHintsVipCore(connector))
                    .build();
            connection.setAutoCommit(false);
            setupRecord(dao, "H", 888888, "H:870970");
            setupRecord(dao, "E", 888888);
            connection.commit();
        }

        AgencyDelete agencyDelete = new AgencyDelete(jdbcUrl, 888888, "http://localhost:" + wireMockServer.port());

        Set<String> nodes = agencyDelete.getIds();

        testArray(nodes, "nodes", "H", "B", "S", "E");
    }

    @Test
    void testRecordOrder() throws Exception {
        {
            RawRepoDAO dao = RawRepoDAO.builder(connection)
                    .relationHints(new RelationHintsVipCore(connector))
                    .build();
            connection.setAutoCommit(false);
            setupRecord(dao, "H-local", 888888);
            setupRecord(dao, "B1", 888888, "H-local:888888");
            setupRecord(dao, "B2", 888888, "H-local:888888");
            connection.commit();
        }

        AgencyDelete agencyDelete = new AgencyDelete(jdbcUrl, 888888, "http://localhost:" + wireMockServer.port());

        assertThat("Sibling size", agencyDelete.getSiblingRelations().size(), is(0));

        Set<String> ids = agencyDelete.getIds();

        assertThat("ids size", ids.size(), is(3));

        Iterator<String> itr = ids.iterator();

        assertThat("Order of records, B2", itr.next(), is("B2"));
        assertThat("Order of records, B1", itr.next(), is("B1"));
        assertThat("Order of records, H", itr.next(), is("H-local"));
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

    private static void setupRecords() throws RawRepoException, SQLException {
        RawRepoDAO dao = RawRepoDAO.builder(connection)
                .relationHints(new RelationHintsVipCore(connector))
                .build();
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

        stmt = connection.prepareStatement("INSERT INTO queuerules(provider, worker, changed, leaf) VALUES(?, ?, ?, ?)");
        stmt.setString(1, "provider");
        stmt.setString(2, "leaf");
        stmt.setString(3, "A");
        stmt.setString(4, "Y");
        stmt.execute();
        stmt.setString(1, "provider");
        stmt.setString(2, "node");
        stmt.setString(3, "Y");
        stmt.setString(4, "A");
        stmt.execute();

        setupRecord(dao, "H", 870970);
        setupRecord(dao, "S", 870970, "H:870970");
        setupRecord(dao, "B", 870970, "S:870970");
        setupRecord(dao, "A", 870971, "B:870970");
        setupRecord(dao, "E", 870970);

        setupRecord(dao, "H", 777777, "H:870970");
        setupRecord(dao, "S", 777777, "S:870970");
        setupRecord(dao, "B", 777777, "B:870970");
        setupRecord(dao, "E", 777777);

        connection.commit();
    }

    private static void setupRecord(RawRepoDAO dao, String bibliographicRecordId, int agencyId, String... relations) throws RawRepoException {
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
        assertThat(message + " size mismatch", ids.size(), is(strings.length));
        for (String string : strings) {
            assertTrue(ids.contains(string));
        }
    }

    private void countQueued(String queue, int count) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("SELECT COUNT(*) FROM queue WHERE worker=?");
        stmt.setString(1, queue);
        ResultSet resultSet = stmt.executeQuery();
        resultSet.next();
        int fromQueue = resultSet.getInt(1);
        assertThat("queue: " + queue, fromQueue, is(count));
    }

    private void countQueued(int agencyId, int count) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("SELECT COUNT(*) FROM queue WHERE agencyid=?");
        stmt.setInt(1, agencyId);
        ResultSet resultSet = stmt.executeQuery();
        resultSet.next();
        int fromQueue = resultSet.getInt(1);
        assertThat("queued for: " + agencyId, fromQueue, is(count));
    }

    private void countQueuedTotal(int count) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("SELECT COUNT(*) FROM queue");
        ResultSet resultSet = stmt.executeQuery();
        resultSet.next();
        int fromQueue = resultSet.getInt(1);
        assertThat("queued total", fromQueue, is(count));
    }

}
