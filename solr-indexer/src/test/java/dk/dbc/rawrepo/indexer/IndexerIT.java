/*
 This file is part of opensearch.
 Copyright © 2013, Dansk Bibliotekscenter a/s,
 Tempovej 7-11, DK-2750 Ballerup, Denmark. CVR: 15149043

 opensearch is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 opensearch is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with opensearch.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dk.dbc.rawrepo.indexer;

import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.Record;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;

/**
 *
 */
public class IndexerIT {

    private static final String PROVIDER = "test";
    private static final String WORKER = "changed";

    private static final String BIBLIOGRAPHIC_RECORD_ID = "A";
    private static final int AGENCY_ID = 870970;

    String jdbcUrl;
    private Connection connection;

    SolrServer solrServer;
    String solrServerUrl;

    @Before
    public void setUp() throws Exception {
        String port = System.getProperty("postgresql.port");
        jdbcUrl = "jdbc:postgresql://localhost:" + port + "/rawrepo";
        connection = DriverManager.getConnection(jdbcUrl);
        connection.setAutoCommit(false);
        resetDatabase();

        solrServerUrl = "http://localhost:" + System.getProperty("jetty.port") + "/solr";
        solrServer = new HttpSolrServer(solrServerUrl);
    }

    @After
    public void tearDown() throws Exception {
        solrServer.deleteByQuery("*:*", 0);
        solrServer.commit(true, true);
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
        stmt.setString(3, BIBLIOGRAPHIC_RECORD_ID);
        stmt.execute();
    }

    private Indexer createInstance() throws SQLException {
        Indexer indexer = createInstance(solrServerUrl);
        indexer.mergerPool = new MergerPool();
        return indexer;
    }

    private Indexer createInstance(String solrUrl) throws SQLException {
        Indexer indexer = new Indexer();
        indexer.mergerPool = new MergerPool();
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(jdbcUrl);
        indexer.dataSource = dataSource;

        indexer.solrUrl = solrUrl;
        indexer.workerName = WORKER;
        indexer.registry = new MetricsRegistry();
        indexer.create();
        return indexer;
    }

    @Test
    public void createRecord() throws Exception {
        RawRepoDAO dao = RawRepoDAO.builder(connection).build();
        assertFalse(dao.recordExists(BIBLIOGRAPHIC_RECORD_ID, AGENCY_ID));

        Record record1 = dao.fetchRecord(BIBLIOGRAPHIC_RECORD_ID, AGENCY_ID);
        record1.setContent("First edition".getBytes());
        record1.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
        dao.saveRecord(record1);
        assertTrue(dao.recordExists(BIBLIOGRAPHIC_RECORD_ID, AGENCY_ID));
        dao.changedRecord(PROVIDER, record1.getId(), "text/marcxchange");
        connection.commit();
        Indexer indexer = createInstance();
        indexer.performWork();
        solrServer.commit(true, true);

        QueryResponse response = solrServer.query(new SolrQuery("rec.agencyId:" + AGENCY_ID));
        assertEquals("Document can be found using library no.", 1, response.getResults().getNumFound());

        response = solrServer.query(new SolrQuery("marc.001b:870971"));
        assertEquals("Document can not be found using different library no.", 0, response.getResults().getNumFound());
    }

    @Test
    public void deleteRecord() throws Exception {
        RawRepoDAO dao = RawRepoDAO.builder(connection).build();
        assertFalse(dao.recordExists(BIBLIOGRAPHIC_RECORD_ID, AGENCY_ID));

        // Put in a document that is going to be deleted
        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", BIBLIOGRAPHIC_RECORD_ID + ":" + AGENCY_ID);
        solrServer.add(document);

        Record record = dao.fetchRecord(BIBLIOGRAPHIC_RECORD_ID, AGENCY_ID);
        record.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
        record.setDeleted(true);
        dao.saveRecord(record);
        dao.changedRecord(PROVIDER, record.getId(), record.getMimeType());
        assertTrue("Record exists", dao.recordExistsMabyDeleted(BIBLIOGRAPHIC_RECORD_ID, AGENCY_ID));
        assertFalse("Record is deleted", dao.recordExists(BIBLIOGRAPHIC_RECORD_ID, AGENCY_ID));

        connection.commit();

        Indexer indexer = createInstance();
        indexer.performWork();
        solrServer.commit(true, true);

        QueryResponse response = solrServer.query(new SolrQuery("id:" + BIBLIOGRAPHIC_RECORD_ID + "\\:" + AGENCY_ID));
        assertEquals("Document can not be found using id. Must have been deleted", 0, response.getResults().getNumFound());
    }

    @Test
    public void createRecordWhenIndexingFails() throws Exception {
        RawRepoDAO dao = RawRepoDAO.builder(connection).build();
        assertFalse(dao.recordExists(BIBLIOGRAPHIC_RECORD_ID, AGENCY_ID));

        Record record1 = dao.fetchRecord(BIBLIOGRAPHIC_RECORD_ID, AGENCY_ID);
        record1.setContent("First edition".getBytes());
        record1.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
        dao.saveRecord(record1);
        assertTrue(dao.recordExists(BIBLIOGRAPHIC_RECORD_ID, AGENCY_ID));
        dao.changedRecord(PROVIDER, record1.getId(), "text/marcxchange");
        connection.commit();

        Indexer indexer = createInstance(solrServerUrl + "X");
        indexer.performWork();
        solrServer.commit(true, true);

        QueryResponse response;
        response = solrServer.query(new SolrQuery("marc.001b:" + AGENCY_ID));
        assertEquals("Document can not be found using library no.", 0, response.getResults().getNumFound());
    }
}
