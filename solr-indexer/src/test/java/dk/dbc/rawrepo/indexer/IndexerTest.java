/*
 * dbc-rawrepo-solr-indexer
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-solr-indexer.
 *
 * dbc-rawrepo-solr-indexer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-solr-indexer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-solr-indexer.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.indexer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class IndexerTest {

    class MockRecord implements Record {

        private final RecordId id;
        private final byte[] content;
        private final Date created;
        private final Date modified;
        private final boolean original;
        private String mimeType;

        MockRecord(String id, int library, byte[] content, Date created, Date modified, boolean original, String mimeType) {
            this.id = new RecordId(id, library);
            this.content = content;
            this.created = created;
            this.modified = modified;
            this.original = original;
            this.mimeType = mimeType;
        }

        @Override
        public byte[] getContent() {
            return this.content;
        }

        @Override
        public boolean isDeleted() {
            return false;
        }

        @Override
        public void setDeleted(boolean deleted) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Date getCreated() {
            return this.created;
        }

        @Override
        public RecordId getId() {
            return this.id;
        }

        @Override
        public Date getModified() {
            return modified;
        }

        @Override
        public boolean isOriginal() {
            return original;
        }

        @Override
        public void setContent(byte[] content) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void setCreated(Date created) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void setModified(Date modified) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public String getMimeType() {
            return mimeType;
        }

        @Override
        public void setMimeType(String mimeType) {
            this.mimeType = mimeType;
        }

        @Override
        public boolean isEnriched() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void setEnriched(boolean enriched) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public String getEnrichmentTrail() {
            return String.valueOf(id.getAgencyId());
        }

        @Override
        public String getTrackingId() {
            return "track";
        }

        @Override
        public void setTrackingId(String trackingId) {
        }
    }

    private static Indexer createInstance() {
        @SuppressWarnings("UseInjectionInsteadOfInstantion")
        Indexer indexer = new Indexer();
        indexer.contentsIndexed = new Counter();
        indexer.contentsSkipped = new Counter();
        indexer.contentsFailed = new Counter();
        return indexer;
    }

    @Test
    public void testCreateIndexDocument() throws IOException, ParserConfigurationException, SAXException, Exception {

        Date created = new Date(100);
        Date modified = new Date(200);
        String content =
                        "<record xmlns='info:lc/xmlns/marcxchange-v1' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' "
                      + "xsi:schemaLocation='http://www.loc.gov/standards/iso25577/marcxchange-1-1.xsd'>\n"
                      + "  <leader>00948cam a2200241 a 4500</leader>\n"
                      + "  <controlfield tag='001'>   94026209 </controlfield>\n"
                      + "  <controlfield tag='003'>DLC</controlfield>\n"
                      + "  <controlfield tag='005'>20180401231613.0</controlfield>\n"
                      + "  <controlfield tag='008'>940606s1995    enka     b    001 0 eng  </controlfield>\n"
                      + "  <datafield tag='040' ind1=' ' ind2=' '>\n"
                      + "    <subfield code='a'>DLC</subfield>\n"
                      + "    <subfield code='c'>DLC</subfield>\n"
                      + "  </datafield>\n"
                      + "  <datafield tag='245' ind1='0' ind2='0'>\n"
                      + "    <subfield code='a'>Research foundations for psychotherapy practice /</subfield>\n"
                      + "    <subfield code='c'>Mark Aveline, David A. Shapiro.</subfield>\n"
                      + "  </datafield>\n"
                      + "  <datafield tag='260' ind1=' ' ind2=' '>\n"
                      + "    <subfield code='a'>Chichester ;</subfield>\n"
                      + "    <subfield code='a'>New York :</subfield>\n"
                      + "    <subfield code='b'>J. Wiley in association with the Mental Health Foundation,</subfield>\n"
                      + "    <subfield code='c'>1995.</subfield>\n"
                      + "  </datafield>\n"
                      + "  <datafield tag='300' ind1=' ' ind2=' '>\n"
                      + "    <subfield code='a'>xiv, 332 p. :</subfield>\n"
                      + "    <subfield code='c'>24 cm.</subfield>\n"
                      + "  </datafield>\n"
                     +  "</record>";

        Record record = new MockRecord("   94026209 ", 100700, content.getBytes(), created, modified, true, MarcXChangeMimeType.MARCXCHANGE);

        Indexer indexer = createInstance();
        indexer.createIndexDocumentTimer = new Timer();
        indexer.worker = new JavaScriptWorker();

        SolrInputDocument doc = indexer.createIndexDocument(record);

        assertEquals(created, doc.getField("rec.created").getValue());
        assertEquals(modified, doc.getField("rec.modified").getValue());
        assertEquals("   94026209 :100700", doc.getField("id").getValue());
        assertEquals("   94026209 ", doc.getField("rec.bibliographicRecordId").getValue());
        assertEquals(100700, doc.getField("rec.agencyId").getValue());

        // check that Marcx record is indexed correctly
        String field003 = (String) doc.getField("marc21.003").getValue();
        String field001 = (String) doc.getField("marc21.001").getValue();
        String field245 = (String) doc.getField("marc21.245a").getValue();
        String field001a001b = (String) doc.getField("marc.001a001b").getValue();

        assertEquals("DLC", field003);
        assertEquals("   94026209 ", field001);
        assertEquals("Research foundations for psychotherapy practice /", field245);
        assertEquals("   94026209 :DLC", field001a001b);

        //TODO fjern - note to self om hvordan testen køres
        //stå i branchen for marc21_proof_of_concept inde i intelliJ
        //kwc@devel8:~/rawrepo$ mvn -pl solr-indexer test -Dtest=IndexerTest

    }
    @Test
    public void testCreateIndexDocument_whenContentCanNotBeParsed() throws IOException, ParserConfigurationException, SAXException, Exception {

        Date created = new Date(100);
        Date modified = new Date(200);
        String content = ">hello world<";

        Record record = new MockRecord("id", 123456, content.getBytes(), created, modified, true, MarcXChangeMimeType.MARCXCHANGE);

        Indexer indexer = createInstance();
        indexer.createIndexDocumentTimer = new Timer();
        indexer.worker = new JavaScriptWorker();

        SolrInputDocument doc = indexer.createIndexDocument(record);

        assertEquals(created, doc.getField("rec.created").getValue());
        assertEquals(modified, doc.getField("rec.modified").getValue());
        assertEquals("id:123456", doc.getField("id").getValue());
        assertEquals("id", doc.getField("rec.bibliographicRecordId").getValue());
        assertEquals(123456, doc.getField("rec.agencyId").getValue());

        // check that Marcx record is not indexed
        assertNull("marc.002a is not present", doc.getField("marc.002a"));
        assertNull("marc.021ae is not present", doc.getField("marc.021ae"));
        assertNull("marc.022a is not present", doc.getField("marc.022a"));
    }

    @Test
    public void testCreateIndexDocument_whenDocumentIsNotMarc() throws IOException, ParserConfigurationException, SAXException, Exception {

        Date created = new Date(100);
        Date modified = new Date(200);
        String content = "";

        Record record = new MockRecord("id", 123456, content.getBytes(), created, modified, true, "DUMMY");

        Indexer indexer = createInstance();
        indexer.createIndexDocumentTimer = new Timer();
        indexer.worker = new JavaScriptWorker();

        SolrInputDocument doc = indexer.createIndexDocument(record);

        assertEquals(created, doc.getField("rec.created").getValue());
        assertEquals(modified, doc.getField("rec.modified").getValue());
        assertEquals("id:123456", doc.getField("id").getValue());
        assertEquals("id", doc.getField("rec.bibliographicRecordId").getValue());
        assertEquals(123456, doc.getField("rec.agencyId").getValue());

        assertNull("marc.002a is not present", doc.getField("marc.002a"));
        assertNull("marc.021ae is not present", doc.getField("marc.021ae"));
        assertNull("marc.022a is not present", doc.getField("marc.022a"));
    }

}
