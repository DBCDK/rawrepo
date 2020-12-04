/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.marcrecord;


import dk.dbc.common.records.MarcField;
import dk.dbc.common.records.MarcRecord;
import dk.dbc.common.records.MarcRecordFactory;
import dk.dbc.common.records.MarcRecordReader;
import dk.dbc.common.records.MarcRecordWriter;
import dk.dbc.common.records.utils.IOUtils;
import dk.dbc.common.records.utils.RecordContentTransformer;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.RawRepoExceptionRecordNotFound;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class ExpandCommonMarcRecordTest {
    private static final String AUT_RAW_26443784 = "authority/raw-26443784.marc";
    private static final String AUT_RAW_52846943 = "authority/raw-52846943.marc";
    private static final String AUT_RAW_53025757 = "authority/raw-53025757.marc";
    private static final String AUT_RAW_53161510 = "authority/raw-53161510.marc";
    private static final String AUT_RAW_53180485 = "authority/raw-53180485.marc";
    private static final String AUT_RAW_53213642 = "authority/raw-53213642.marc";
    private static final String AUT_RAW_53214592 = "authority/raw-53214592.marc";
    private static final String AUT_RAW_53214827 = "authority/raw-53214827.marc";
    private static final String AUT_RAW_90004158 = "authority/raw-90004158.marc";
    private static final String AUT_RAW_126350333 = "authority/raw-126350333.marc";
    private static final String AUT_RAW_126850298 = "authority/raw-126850298.marc";

    private static final String AUT_EXPANDED_26443784 = "authority/expanded-26443784.marc";
    private static final String AUT_EXPANDED_52846943 = "authority/expanded-52846943.marc";
    private static final String AUT_EXPANDED_53025757 = "authority/expanded-53025757.marc";
    private static final String AUT_EXPANDED_53161510 = "authority/expanded-53161510.marc";
    private static final String AUT_EXPANDED_53180485 = "authority/expanded-53180485.marc";
    private static final String AUT_EXPANDED_53213642 = "authority/expanded-53213642.marc";
    private static final String AUT_EXPANDED_53214592 = "authority/expanded-53214592.marc";
    private static final String AUT_EXPANDED_53214827 = "authority/expanded-53214827.marc";
    private static final String AUT_EXPANDED_90004158 = "authority/expanded-90004158.marc";
    private static final String AUT_EXPANDED_126350333 = "authority/expanded-126350333.marc";
    private static final String AUT_EXPANDED_126850298 = "authority/expanded-126850298.marc";

    private static final String AUTHORITY_19024687 = "authority/authority-19024687.marc";
    private static final String AUTHORITY_19024709 = "authority/authority-19024709.marc";
    private static final String AUTHORITY_19043800 = "authority/authority-19043800.marc";
    private static final String AUTHORITY_19064689 = "authority/authority-19064689.marc";
    private static final String AUTHORITY_19130452 = "authority/authority-19130452.marc";
    private static final String AUTHORITY_68098203 = "authority/authority-68098203.marc";
    private static final String AUTHORITY_68058953 = "authority/authority-68058953.marc";
    private static final String AUTHORITY_68313686 = "authority/authority-68313686.marc";
    private static final String AUTHORITY_68354153 = "authority/authority-68354153.marc";
    private static final String AUTHORITY_68432359 = "authority/authority-68432359.marc";
    private static final String AUTHORITY_68472806 = "authority/authority-68472806.marc";
    private static final String AUTHORITY_68560985 = "authority/authority-68560985.marc";
    private static final String AUTHORITY_68570492 = "authority/authority-68570492.marc";
    private static final String AUTHORITY_68584566 = "authority/authority-68584566.marc";
    private static final String AUTHORITY_68585627 = "authority/authority-68585627.marc";
    private static final String AUTHORITY_68712742 = "authority/authority-68712742.marc";
    private static final String AUTHORITY_68839734 = "authority/authority-68839734.marc";
    private static final String AUTHORITY_68895650 = "authority/authority-68895650.marc";
    private static final String AUTHORITY_68900719 = "authority/authority-68900719.marc";
    private static final String AUTHORITY_69094139 = "authority/authority-69094139.marc";
    private static final String AUTHORITY_69294685 = "authority/authority-69294685.marc";
    private static final String AUTHORITY_69328776 = "authority/authority-69328776.marc";
    private static final String AUTHORITY_68046335 = "authority/authority-68046335.marc";
    private static final String AUTHORITY_68139864 = "authority/authority-68139864.marc";
    private static final String AUTHORITY_68619858 = "authority/authority-68619858.marc";
    private static final String AUTHORITY_68679265 = "authority/authority-68679265.marc";

    private static final String COMMON_SINGLE_RECORD_RESOURCE = "authority/common_enrichment.marc";

    private static MarcRecord loadMarcRecord(String filename) throws IOException {
        InputStream is = ExpandCommonMarcRecordTest.class.getResourceAsStream("/" + filename);
        return MarcRecordFactory.readRecord(IOUtils.readAll(is, "UTF-8"));
    }

    @Test
    public void expandCommonRecordOk_52846943() throws Exception {
        MarcRecord raw = loadMarcRecord(AUT_RAW_52846943);
        MarcRecord expanded = loadMarcRecord(AUT_EXPANDED_52846943);
        MarcRecord auth1 = loadMarcRecord(AUTHORITY_19024709);
        MarcRecord auth2 = loadMarcRecord(AUTHORITY_19024687);

        Map<String, MarcRecord> collection = new HashMap<>();
        collection.put("5 284 694 3", raw);
        collection.put("19024709", auth1);
        collection.put("19024687", auth2);

        assertThat(sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "5 284 694 3")), equalTo(expanded));
    }

    @Test
    public void expandCommonRecordOk_53025757() throws Exception {
        MarcRecord raw = loadMarcRecord(AUT_RAW_53025757);
        MarcRecord expanded = loadMarcRecord(AUT_EXPANDED_53025757);
        MarcRecord auth1 = loadMarcRecord(AUTHORITY_68432359);
        MarcRecord auth2 = loadMarcRecord(AUTHORITY_69328776);
        MarcRecord auth3 = loadMarcRecord(AUTHORITY_19043800);

        Map<String, MarcRecord> collection = new HashMap<>();
        collection.put("5 302 575 7", raw);
        collection.put("68432359", auth1);
        collection.put("69328776", auth2);
        collection.put("19043800", auth3);

        assertThat(sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "5 302 575 7")), equalTo(expanded));
    }

    @Test
    public void expandCommonRecordOk_53161510() throws Exception {
        MarcRecord raw = loadMarcRecord(AUT_RAW_53161510);
        MarcRecord expanded = loadMarcRecord(AUT_EXPANDED_53161510);
        MarcRecord auth1 = loadMarcRecord(AUTHORITY_69094139);
        MarcRecord auth2 = loadMarcRecord(AUTHORITY_68098203);
        MarcRecord auth3 = loadMarcRecord(AUTHORITY_19064689);

        Map<String, MarcRecord> collection = new HashMap<>();
        collection.put("5 316 151 0", raw);
        collection.put("69094139", auth1);
        collection.put("68098203", auth2);
        collection.put("19064689", auth3);

        assertThat(sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "5 316 151 0")), equalTo(expanded));
    }

    @Test
    public void expandCommonRecordOk_53180485() throws Exception {
        MarcRecord raw = loadMarcRecord(AUT_RAW_53180485);
        MarcRecord expanded = loadMarcRecord(AUT_EXPANDED_53180485);
        MarcRecord auth1 = loadMarcRecord(AUTHORITY_68839734);
        MarcRecord auth2 = loadMarcRecord(AUTHORITY_68584566);
        MarcRecord auth3 = loadMarcRecord(AUTHORITY_68900719);
        MarcRecord auth4 = loadMarcRecord(AUTHORITY_68560985);

        Map<String, MarcRecord> collection = new HashMap<>();
        collection.put("5 318 048 5", raw);
        collection.put("68839734", auth1);
        collection.put("68584566", auth2);
        collection.put("68900719", auth3);
        collection.put("68560985", auth4);

        assertThat(sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "5 318 048 5")), equalTo(expanded));
    }

    @Test
    public void expandCommonRecordOk_53213642() throws Exception {
        MarcRecord raw = loadMarcRecord(AUT_RAW_53213642);
        MarcRecord expanded = loadMarcRecord(AUT_EXPANDED_53213642);
        MarcRecord auth1 = loadMarcRecord(AUTHORITY_68895650);
        MarcRecord auth2 = loadMarcRecord(AUTHORITY_19130452);

        Map<String, MarcRecord> collection = new HashMap<>();
        collection.put("5 321 364 2", raw);
        collection.put("68895650", auth1);
        collection.put("19130452", auth2);

        assertThat(sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "5 321 364 2")), equalTo(expanded));
    }

    @Test
    public void expandCommonRecordOk_53214592() throws Exception {
        MarcRecord raw = loadMarcRecord(AUT_RAW_53214592);
        MarcRecord expanded = loadMarcRecord(AUT_EXPANDED_53214592);
        MarcRecord auth1 = loadMarcRecord(AUTHORITY_68354153);
        MarcRecord auth3 = loadMarcRecord(AUTHORITY_68472806);
        MarcRecord auth4 = loadMarcRecord(AUTHORITY_68585627);

        Map<String, MarcRecord> collection = new HashMap<>();
        collection.put("5 321 459 2", raw);
        collection.put("68354153", auth1);
        collection.put("68472806", auth3);
        collection.put("68585627", auth4);

        assertThat(sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "5 321 459 2")), equalTo(expanded));
    }

    @Test
    public void expandCommonRecordOk_53214827() throws Exception {
        MarcRecord raw = loadMarcRecord(AUT_RAW_53214827);
        MarcRecord expanded = loadMarcRecord(AUT_EXPANDED_53214827);
        MarcRecord auth1 = loadMarcRecord(AUTHORITY_68570492);

        Map<String, MarcRecord> collection = new HashMap<>();
        collection.put("5 321 482 7", raw);
        collection.put("68570492", auth1);

        assertThat(sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "5 321 482 7")), equalTo(expanded));
    }

    @Test
    public void expandCommonRecordOk_90004158() throws Exception {
        MarcRecord raw = loadMarcRecord(AUT_RAW_90004158);
        MarcRecord expanded = loadMarcRecord(AUT_EXPANDED_90004158);
        MarcRecord auth1 = loadMarcRecord(AUTHORITY_68712742);
        MarcRecord auth2 = loadMarcRecord(AUTHORITY_69294685);

        Map<String, MarcRecord> collection = new HashMap<>();
        collection.put("90004158", raw);
        collection.put("68712742", auth1);
        collection.put("69294685", auth2);

        assertThat(sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "90004158")), equalTo(expanded));
    }

    @Test
    public void noCommonRecord() throws Exception {
        Map<String, MarcRecord> collection = new HashMap<>();

        Assertions.assertThrows(RawRepoException.class, () -> sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "")));
    }

    @Test
    public void missingAuthorityRecords() throws Exception {
        MarcRecord record = loadMarcRecord(AUT_RAW_90004158);

        Map<String, MarcRecord> collection = new HashMap<>();
        collection.put("90004158", record);

        Assertions.assertThrows(RawRepoException.class, () -> sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "90004158")));
    }

    @Test
    public void expandCommonRecordWithoutAuthorityFields() throws Exception {
        MarcRecord record = loadMarcRecord(COMMON_SINGLE_RECORD_RESOURCE);

        Map<String, MarcRecord> collection = new HashMap<>();
        collection.put("20611529", record);

        assertThat(sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "20611529")), equalTo(record));
    }

    @Test
    public void expandCommonRecordWithTwoReferencesToSameAuthorityRecord() throws Exception {
        MarcRecord raw = loadMarcRecord(AUT_RAW_26443784);
        MarcRecord authority = loadMarcRecord(AUTHORITY_68313686);
        MarcRecord expanded = loadMarcRecord(AUT_EXPANDED_26443784);

        Map<String, MarcRecord> collection = new HashMap<>();
        collection.put("26443784", raw);
        collection.put("68313686", authority);

        assertThat(sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "26443784")), equalTo(expanded));
    }

    @Test
    public void expandLittolkRecordWithDoubleA() throws Exception {
        MarcRecord raw = loadMarcRecord(AUT_RAW_126350333);

        MarcRecord authority1 = loadMarcRecord(AUTHORITY_68058953);
        MarcRecord authority2 = loadMarcRecord(AUTHORITY_68560985);
        MarcRecord expanded = loadMarcRecord(AUT_EXPANDED_126350333);

        Map<String, MarcRecord> collection = new HashMap<>();
        collection.put("126350333", raw);

        collection.put("68058953", authority1);
        collection.put("68560985", authority2);

        assertThat(sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "126350333")), equalTo(expanded));
    }

    @Test
    public void expandLittolkRecordWithExistingReferenceEarlierName() throws Exception {
        MarcRecord raw = loadMarcRecord(AUT_RAW_126850298);

        MarcRecord authority1 = loadMarcRecord(AUTHORITY_68046335);
        MarcRecord authority2 = loadMarcRecord(AUTHORITY_68139864);
        MarcRecord authority3 = loadMarcRecord(AUTHORITY_68619858);
        MarcRecord authority4 = loadMarcRecord(AUTHORITY_68679265);

        MarcRecord expanded = loadMarcRecord(AUT_EXPANDED_126850298);

        Map<String, MarcRecord> collection = new HashMap<>();
        collection.put("126850298", raw);

        collection.put("68046335", authority1);
        collection.put("68139864", authority2);
        collection.put("68619858", authority3);
        collection.put("68679265", authority4);

        assertThat(sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "126850298")), equalTo(expanded));
    }

    @Test
    public void expandLittolkRecordWithExistingReferenceLaterName() throws Exception {
        MarcRecord raw = loadMarcRecord(AUT_RAW_126850298);

        MarcRecord authority1 = loadMarcRecord(AUTHORITY_68046335);
        MarcRecord authority2 = loadMarcRecord(AUTHORITY_68139864);
        MarcRecord authority3 = loadMarcRecord(AUTHORITY_68619858);
        MarcRecord authority4 = loadMarcRecord(AUTHORITY_68679265);

        MarcRecordWriter authority3RecordWriter = new MarcRecordWriter(authority3);
        authority3RecordWriter.addOrReplaceSubfield("500", "w", "senere navn");

        MarcRecord expanded = loadMarcRecord(AUT_EXPANDED_126850298);
        expanded.getFields().get(19).getSubfields().get(2).setValue("se også under det tidligere navn");

        Map<String, MarcRecord> collection = new HashMap<>();
        collection.put("126850298", raw);

        collection.put("68046335", authority1);
        collection.put("68139864", authority2);
        collection.put("68619858", authority3);
        collection.put("68679265", authority4);

        assertThat(sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "126850298")), equalTo(expanded));
    }

    @Test
    public void expandLittolkRecordWithExistingReferenceCustomText() throws Exception {
        MarcRecord raw = loadMarcRecord(AUT_RAW_126850298);

        MarcRecord authority1 = loadMarcRecord(AUTHORITY_68046335);
        MarcRecord authority2 = loadMarcRecord(AUTHORITY_68139864);
        MarcRecord authority3 = loadMarcRecord(AUTHORITY_68619858);
        MarcRecord authority4 = loadMarcRecord(AUTHORITY_68679265);

        MarcRecordWriter authority3RecordWriter = new MarcRecordWriter(authority3);
        authority3RecordWriter.addOrReplaceSubfield("500", "w", "også kendt som");

        MarcRecord expanded = loadMarcRecord(AUT_EXPANDED_126850298);
        expanded.getFields().get(19).getSubfields().get(2).setValue("også kendt som");

        Map<String, MarcRecord> collection = new HashMap<>();
        collection.put("126850298", raw);

        collection.put("68046335", authority1);
        collection.put("68139864", authority2);
        collection.put("68619858", authority3);
        collection.put("68679265", authority4);

        assertThat(sortRecord(ExpandCommonMarcRecord.expandMarcRecord(collection, "126850298")), equalTo(expanded));
    }

    private MarcRecord sortRecord(MarcRecord record) {
        Collections.sort(record.getFields(), new Comparator<MarcField>() {
            public int compare(MarcField o1, MarcField o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });

        return record;
    }

    private static Record recordFromContent(final MarcRecord marcRecord) throws Exception {
        final MarcRecordReader reader = new MarcRecordReader(marcRecord);
        final String id = reader.getRecordId();
        final int agencyId = reader.getAgencyIdAsInt();

        return new Record() {
            boolean deleted = false;
            boolean enriched = false;
            byte[] content = RecordContentTransformer.encodeRecord(marcRecord);
            String trackingId = "Track-" + id + ":" + Integer.toString(agencyId);
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
    public void testExpandRecord_1() throws Exception {
        Record raw = recordFromContent(loadMarcRecord(AUT_RAW_52846943));
        Record expanded = recordFromContent(loadMarcRecord(AUT_EXPANDED_52846943));
        Record auth1 = recordFromContent(loadMarcRecord(AUTHORITY_19024709));
        Record auth2 = recordFromContent(loadMarcRecord(AUTHORITY_19024687));

        Map<String, Record> collection = new HashMap<>();

        collection.put("19024709", auth1);
        collection.put("19024687", auth2);

        ExpandCommonMarcRecord.expandRecord(raw, collection);

        assertThat(RecordContentTransformer.decodeRecord(raw.getContent()), equalTo(RecordContentTransformer.decodeRecord(expanded.getContent())));
        assertThat(raw.getId(), equalTo(expanded.getId()));
        assertThat(raw.getMimeType(), equalTo(expanded.getMimeType()));
        assertThat(raw.getTrackingId(), equalTo(expanded.getTrackingId()));
        assertThat(raw.getCreated(), equalTo(expanded.getCreated()));
    }
}
