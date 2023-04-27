package dk.dbc.marcrecord;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.DanMarc2LineFormatReader;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.writer.MarcXchangeV1Writer;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

class ExpandCommonRecordTest {
    private static final String AUTHORITY_19024687 = "authority/authority-19024687.marc";
    private static final String AUTHORITY_19024709 = "authority/authority-19024709.marc";
    private static final String AUT_EXPANDED_52846943 = "authority/expanded-52846943.marc";
    private static final String AUT_RAW_52846943 = "authority/raw-52846943.marc";

    private static final MarcXchangeV1Writer writer = new MarcXchangeV1Writer();

    private static MarcRecord loadMarcRecord(String filename) throws MarcReaderException, IOException {
        final ClassLoader classLoader = ExpandCommonRecordTest.class.getClassLoader();
        final File file = new File(Objects.requireNonNull(classLoader.getResource(filename)).getFile());
        final InputStream is = new FileInputStream(file);

        final DanMarc2LineFormatReader lineFormatReader = new DanMarc2LineFormatReader(is, StandardCharsets.UTF_8);

        return lineFormatReader.read();
    }

    private static MarcRecord decodeRecord(byte[] content) throws MarcReaderException {
        final ByteArrayInputStream is = new ByteArrayInputStream(content);

        final DanMarc2LineFormatReader lineFormatReader = new DanMarc2LineFormatReader(is, StandardCharsets.UTF_8);

        return lineFormatReader.read();
    }

    private static Record recordFromContent(final MarcRecord marcRecord) throws Exception {
        final String id = marcRecord.getSubFieldValue("001", 'a').orElseThrow();
        final int agencyId = Integer.parseInt(marcRecord.getSubFieldValue("001", 'b').orElseThrow());

        return new Record() {
            boolean deleted = false;
            boolean enriched = false;
            byte[] content = writer.write(marcRecord, StandardCharsets.UTF_8);
            final String trackingId = "Track-" + id + ":" + agencyId;
            String mimeType = findMimeType();

            private String findMimeType() {
                if (agencyId == 870970)
                    return MarcXChangeMimeType.MARCXCHANGE;
                else if (agencyId == 870971)
                    return MarcXChangeMimeType.ARTICLE;
                else if (agencyId == 870979)
                    return MarcXChangeMimeType.AUTHORITY;
                else
                    return MarcXChangeMimeType.ENRICHMENT;
            }

            @Override
            public byte[] getContent() {
                return content;
            }

            @Override
            public boolean isDeleted() {
                return deleted;
            }

            @Override
            public void setDeleted(boolean deleted) {
                this.deleted = deleted;
            }

            @Override
            public void setContent(byte[] content) {
                this.content = content;
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
            public Instant getCreated() {
                return Instant.now();
            }

            @Override
            public void setCreated(Instant created) {
            }

            @Override
            public RecordId getId() {
                return new RecordId(id, agencyId);
            }

            @Override
            public Instant getModified() {
                return Instant.now();
            }

            @Override
            public void setModified(Instant modified) {
            }

            @Override
            public boolean isOriginal() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public String toString() {
                return "{" + content + '}';
            }

            @Override
            public boolean isEnriched() {
                return enriched;
            }

            @Override
            public void setEnriched(boolean enriched) {
                this.enriched = enriched;
            }

            @Override
            public String getEnrichmentTrail() {
                return String.valueOf(agencyId);
            }

            @Override
            public String getTrackingId() {
                return trackingId;
            }

            @Override
            public void setTrackingId(String trackingId) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
    }

    @Test
    void testExpandRecord_1() throws Exception {
        Record raw = recordFromContent(loadMarcRecord(AUT_RAW_52846943));
        Record expanded = recordFromContent(loadMarcRecord(AUT_EXPANDED_52846943));
        Record auth1 = recordFromContent(loadMarcRecord(AUTHORITY_19024709));
        Record auth2 = recordFromContent(loadMarcRecord(AUTHORITY_19024687));

        Map<String, Record> collection = new HashMap<>();

        collection.put("19024709", auth1);
        collection.put("19024687", auth2);

        ExpandCommonRecord.expandRecord(raw, collection);

        assertThat(decodeRecord(raw.getContent()), equalTo(decodeRecord(expanded.getContent())));
        assertThat(raw.getId(), equalTo(expanded.getId()));
        assertThat(raw.getMimeType(), equalTo(expanded.getMimeType()));
        assertThat(raw.getTrackingId(), equalTo(expanded.getTrackingId()));
        assertThat(raw.getCreated().truncatedTo(ChronoUnit.SECONDS), equalTo(expanded.getCreated().truncatedTo(ChronoUnit.SECONDS)));
    }
}
