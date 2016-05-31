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
import java.io.IOException;
import java.util.Date;
import java.util.List;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.solr.common.SolrInputDocument;

import static org.junit.Assert.*;

import org.junit.Test;
import org.xml.sax.SAXException;

/**
 *
 * @author DBC <dbc.dk>
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
        String content = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                         + "<marcx:record format=\"danMARC2\" type=\"Bibliographic\" xmlns:marcx=\"info:lc/xmlns/marcxchange-v1\">\n"
                         + "      <marcx:leader>00000n    2200000   4500</marcx:leader>\n"
                         + "      <marcx:datafield tag=\"001\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">2 364 149 6</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"b\">191919</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"c\">20130118221234</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"d\">20010822</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"f\">a</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"002\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">06605141</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"002\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"b\">810010</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"d\">09009310</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"x\">81001009009310</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"004\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"r\">n</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"a\">b</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"008\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"t\">m</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"u\">f</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"a\">2001</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"l\">dan</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"v\">0</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"014\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">2 364 143 7</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield ind1=\"0\" ind2=\"0\" tag=\"021\">\n"
                         + "        <marcx:subfield code=\"e\">9788777248573</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"022\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">0904-6054</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"c\">hf.</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"d\">kr. 6,50 pr. nr.</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"023\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">23-a</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"b\">23-b</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"032\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">IDO200137</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"x\">NET200137</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"x\">DAT991304</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"245\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"g\">1.3</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"a\">Forandringsledelse og orkestrering</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"e\">udarbejdet af: Danmarks Tekniske Universitet, Byg.DTU og Institut for Produktion og Ledelse</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"e\">forfatter: Peter Vogelius, Christian Koch</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"526\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"i\">Hertil findes</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"t\">Bilag</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"u\">http://www.arbejdsulykker.dk/pdf/1_3_bilag.pdf</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"532\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">Med litteraturhenvisninger</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"700\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"0\"/>\n"
                         + "        <marcx:subfield code=\"a\">Vogelius</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"h\">Peter</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"700\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"0\"/>\n"
                         + "        <marcx:subfield code=\"a\">Koch</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"h\">Christian</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"c\">f. 1958</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"710\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"Ã¥\">2</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"a\">Danmarks Tekniske Universitet</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"c\">BYG. DTU</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"710\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"Ã¥\">1</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"a\">Danmarks Tekniske Universitet</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"c\">Institut for Produktion og Ledelse</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"856\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"z\">AdgangsmÃ¥de: Internet</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"u\">http://www.arbejdsulykker.dk/pdf/met_1_3.pdf</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"z\">KrÃ¦ver lÃ¦seprogrammet Acrobat Reader</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"910\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">BYG. DTU, Danmarks Tekniske Universitet</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"z\">710/2</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"910\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">DTU</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"z\">710/1(a)</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"910\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">Institut for Produktion og Ledelse, Danmarks Tekniske Universitet</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"z\">710/1</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"910\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">IPL</marcx:subfield>\n"
                         + "        <marcx:subfield code=\"z\">710/1</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"d08\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">tb</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"d08\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">rettet i forb. med tilf. af 008w</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"s10\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">DBC</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"z99\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">masseret</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "      <marcx:datafield tag=\"n55\" ind1=\"0\" ind2=\"0\">\n"
                         + "        <marcx:subfield code=\"a\">20050302</marcx:subfield>\n"
                         + "      </marcx:datafield>\n"
                         + "    </marcx:record>";

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

        // check that Marcx record is indexed correctly
        String field002a = (String) doc.getField("marc.002a").getValue();
        String field022a = (String) doc.getField("marc.022a").getValue();

        assertEquals("06605141", field002a);
        assertEquals("0904-6054", field022a);

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
