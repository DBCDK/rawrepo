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

import java.io.IOException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class SchemaValidationIT {

    private static final String MODIFIED = "2005-12-31T23:59:59Z";
    private static final String CREATED = "2000-01-01T12:34:56Z";
    private static final String FIELD_002A = "1 234 567 8";
    private static final String FIELD_021AE = "123456789X";
    private static final String FIELD_022A = "1234-5678";
    private static final String AGENCY = "870970";
    private static final String DOCUMENT_ID = "1 234 432 1";
    private static final String FIELD_COLLECTION = "dbc";
    private static final String REC_TRACKING_ID = "abc123";

    private static SolrServer solrServer;
    private static String solrServerUrl;

    @Before
    public void setUp() throws Exception {
        solrServerUrl = "http://localhost:" + System.getProperty("jetty.port") + "/solr";

        solrServer = new HttpSolrServer(solrServerUrl);
    }

    @After
    public void tearDown() throws Exception {
        solrServer.deleteByQuery("*:*");
        solrServer.commit(true, true);
    }

    private void addTestDocument() throws SolrServerException, IOException {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", DOCUMENT_ID + ":" + AGENCY);
        doc.addField("rec.bibliographicRecordId", DOCUMENT_ID);
        doc.addField("rec.agencyId", AGENCY);
        doc.addField("marc.002a", FIELD_002A);
        doc.addField("marc.021ae", FIELD_021AE);
        doc.addField("marc.022a", FIELD_022A);
        doc.addField("rec.collectionIdentifier", FIELD_COLLECTION);
        doc.addField("rec.created", CREATED);
        doc.addField("rec.modified", MODIFIED);
        doc.addField("rec.trackingId", REC_TRACKING_ID);
        solrServer.add(doc);
        solrServer.commit(true, true);
    }

    @Test
    public void verifyConnectivity() throws Exception {
        // Solr server must be able to start up with the supplied schema and configuration
        solrServer.ping();
    }

    @Test
    public void testAddedDocumentIsSearchableWithExactSearches() throws Exception {
        addTestDocument();
        QueryResponse response;

        response = solrServer.query(new SolrQuery("marc.002a:\"" + FIELD_002A + "\""));
        Assert.assertEquals(1, response.getResults().getNumFound());

        response = solrServer.query(new SolrQuery("marc.021ae:\"" + FIELD_021AE + "\""));
        Assert.assertEquals(1, response.getResults().getNumFound());

        response = solrServer.query(new SolrQuery("marc.022a:\"" + FIELD_022A + "\""));
        Assert.assertEquals(1, response.getResults().getNumFound());
    }

    @Test
    public void testAddedDocumentIsSearchableInNormalizedForm() throws Exception {
        addTestDocument();
        QueryResponse response;

        response = solrServer.query(new SolrQuery("analyzedId:\"" + "12344321" + "\\:" + AGENCY + "\""));
        Assert.assertEquals(1, response.getResults().getNumFound());

        response = solrServer.query(new SolrQuery("rec.bibliographicRecordId:12344321"));
        Assert.assertEquals(1, response.getResults().getNumFound());

        response = solrServer.query(new SolrQuery("marc.002a:12345678"));
        Assert.assertEquals(1, response.getResults().getNumFound());

        response = solrServer.query(new SolrQuery("marc.022a:12345678"));
        Assert.assertEquals(1, response.getResults().getNumFound());
    }

    // Is search supposed to be case insensitive ?
    @Test
    public void testAddedDocumentIsNotSearchableUsingDifferentCase() throws Exception {
        addTestDocument();
        QueryResponse response;

        response = solrServer.query(new SolrQuery("id:Document\\:870970"));
        Assert.assertEquals(0, response.getResults().getNumFound());

        response = solrServer.query(new SolrQuery("rec.bibliographicRecordId:Document"));
        Assert.assertEquals(0, response.getResults().getNumFound());

        response = solrServer.query(new SolrQuery("marc.021ae:123456789x"));
        Assert.assertEquals(0, response.getResults().getNumFound());
    }

}
