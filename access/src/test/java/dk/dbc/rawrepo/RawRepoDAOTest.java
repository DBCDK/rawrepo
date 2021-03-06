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
package dk.dbc.rawrepo;

import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.vipcore.exception.VipCoreException;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class RawRepoDAOTest {

    public RawRepoDAOTest() {
    }

    private VipCoreLibraryRulesConnector getVipCoreConnector() throws VipCoreException {
        VipCoreLibraryRulesConnector vipCoreLibraryRulesConnectorMock = mock(VipCoreLibraryRulesConnector.class);
        when(vipCoreLibraryRulesConnectorMock.hasFeature(eq(870970), eq(VipCoreLibraryRulesConnector.Rule.USE_ENRICHMENTS))).thenReturn(true);
        when(vipCoreLibraryRulesConnectorMock.hasFeature(eq(870975), eq(VipCoreLibraryRulesConnector.Rule.USE_ENRICHMENTS))).thenReturn(true);
        when(vipCoreLibraryRulesConnectorMock.hasFeature(eq(1), eq(VipCoreLibraryRulesConnector.Rule.USE_ENRICHMENTS))).thenReturn(true);
        when(vipCoreLibraryRulesConnectorMock.hasFeature(eq(2), eq(VipCoreLibraryRulesConnector.Rule.USE_ENRICHMENTS))).thenReturn(false);

        return vipCoreLibraryRulesConnectorMock;
    }

    @Test
    public void testEnrichmentTrail() throws Exception {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.relationHints = new RelationHintsVipCore(getVipCoreConnector());
            doCallRealMethod().when(access).fetchMergedRecord(anyString(), anyInt(), any(MarcXMerger.class), anyBoolean());
            doCallRealMethod().when(access).agencyFor(anyString(), anyInt(), anyBoolean());
            fillMockRelations(access,
                    "B:870970", // HEAD
                    "C:870970", // SECTION
                    "D:870970", "D:1", "D:2", // BIND
                    "E:870970", "E:1", "E:2", // BIND
                    "F:870970", // SECTION
                    "G:870970", "G:1", "G:2", // BIND
                    "H:870970", "H:1", "H:2");// BIND
            MarcXMerger marcXMerger = new MarcXMerger() {
                @Override
                public byte[] merge(byte[] common, byte[] local, boolean includeAllFields) {
                    return common;
                }
            };

            assertThat(access.fetchMergedRecord("D", 870970, marcXMerger, true).getEnrichmentTrail(), is("870970"));
            assertThat(access.fetchMergedRecord("D", 1, marcXMerger, true).getEnrichmentTrail(), is("870970,1"));

        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

    }

    @Test
    public void testFetchRecordCollection() throws Exception {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);

            access.relationHints = new RelationHintsVipCore(getVipCoreConnector());

            doCallRealMethod().when(access).fetchRecordCollection(anyString(), anyInt(), any(MarcXMerger.class));
            doCallRealMethod().when(access).agencyFor(anyString(), anyInt(), anyBoolean());
            doCallRealMethod().when(access).fetchMergedRecord(anyString(), anyInt(), any(MarcXMerger.class), anyBoolean());
            doCallRealMethod().when(access).findParentRelationAgency(anyString(), anyInt());
            fillMockRelations(access,
                    "A:870970", "A:1", "A:2",
                    "B:870970", // HEAD
                    "C:870970", "C:1", // SECTION
                    "D:870970", // BIND
                    "E:870970", "E:2", // BIND
                    "F:870970", "F:2", // SECTION
                    "G:870970", "G:1", // BIND
                    "H:870970", // BIND
                    "I:870975");
            System.out.println("access = " + access);
            MarcXMerger merger = new MarcXMerger() {

                @Override
                public byte[] merge(byte[] common, byte[] local, boolean isFinal) {
                    return local;
                }

            };
            recordCollectionIs(access.fetchRecordCollection("A", 870970, merger), "A:870970"); // ENTITY
            recordCollectionIs(access.fetchRecordCollection("A", 1, merger), "A:1"); // ENTITY LOCAL
            recordCollectionIs(access.fetchRecordCollection("D", 870970, merger), "D:870970", "C:870970", "B:870970"); // BIND
            recordCollectionIs(access.fetchRecordCollection("D", 1, merger), "D:870970", "C:1", "B:870970"); // BIND LOCAL SECTION
            recordCollectionIs(access.fetchRecordCollection("G", 1, merger), "G:1", "F:870970", "B:870970"); // BIND LOCAL BIND
            recordCollectionIs(access.fetchRecordCollection("F", 2, merger), "F:2"); // NO COMMON AGENCY
            recordCollectionIs(access.fetchRecordCollection("I", 870975, merger), "I:870975", "D:870970", "C:870970", "B:870970");

        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void parentRelationAgency() throws Exception {
        RawRepoDAO mock = mock(RawRepoDAO.class);
        mock.relationHints = mock(RelationHintsVipCore.class);
        doCallRealMethod().when(mock).findParentRelationAgency(anyString(), anyInt());
        when(mock.relationHints.get(123456)).thenReturn(Arrays.asList(300000, 870970));
        when(mock.relationHints.usesCommonAgency(123456)).thenReturn(Boolean.TRUE);
        when(mock.relationHints.usesCommonAgency(654321)).thenReturn(Boolean.FALSE);
        when(mock.recordExists(anyString(), anyInt())).thenReturn(Boolean.FALSE);
        when(mock.recordExists("PRIVATE", 123456)).thenReturn(Boolean.TRUE);
        when(mock.recordExists("PRIVATE", 654321)).thenReturn(Boolean.TRUE);
        when(mock.recordExists("COMMON", 870970)).thenReturn(Boolean.TRUE);
        when(mock.recordExists("COMMON", 654321)).thenReturn(Boolean.TRUE);
        int parentRelationAgency;
        parentRelationAgency = mock.findParentRelationAgency("PRIVATE", 123456);
        assertThat(parentRelationAgency, is(123456));
        parentRelationAgency = mock.findParentRelationAgency("COMMON", 123456);
        assertThat(parentRelationAgency, is(870970));
        parentRelationAgency = mock.findParentRelationAgency("PRIVATE", 654321);
        assertThat(parentRelationAgency, is(654321));
        parentRelationAgency = mock.findParentRelationAgency("COMMON", 654321);
        assertThat(parentRelationAgency, is(654321));
        try {
            mock.findParentRelationAgency("NA", 123456);
            fail("Expected RawRepoExceptionRecordNotFound");
        } catch (RawRepoExceptionRecordNotFound ex) {
            System.out.println("Didn't find record " + ex.getMessage());
        }
    }

    @Test
    public void siblingRelationAgency() throws Exception {
        RawRepoDAO mock = mock(RawRepoDAO.class);
        mock.relationHints = mock(RelationHintsVipCore.class);
        doCallRealMethod().when(mock).findSiblingRelationAgency(anyString(), anyInt());
        when(mock.relationHints.get(123456)).thenReturn(Arrays.asList(300000, 870970));
        when(mock.relationHints.usesCommonAgency(123456)).thenReturn(Boolean.TRUE);
        when(mock.recordExists(anyString(), anyInt())).thenReturn(Boolean.FALSE);
        when(mock.recordExists("COMMON", 870970)).thenReturn(Boolean.TRUE);
        when(mock.recordExists("INTERM", 300000)).thenReturn(Boolean.TRUE);
        int siblingRelationAgency;
        siblingRelationAgency = mock.findSiblingRelationAgency("COMMON", 123456);
        System.out.println("parentRelationAgency = " + siblingRelationAgency);
        assertThat(siblingRelationAgency, is(870970));
        siblingRelationAgency = mock.findSiblingRelationAgency("INTERM", 123456);
        assertThat(siblingRelationAgency, is(300000));
        try {
            mock.findSiblingRelationAgency("PRIVATE", 123456);
            fail("Expected RawRepoExceptionRecordNotFound");
        } catch (RawRepoExceptionRecordNotFound ex) {
            System.out.println("Didn't find record " + ex.getMessage());
        }
    }

    //  _   _      _                   _____                 _   _
    // | | | | ___| |_ __   ___ _ __  |  ___|   _ _ __   ___| |_(_) ___  _ __  ___
    // | |_| |/ _ \ | '_ \ / _ \ '__| | |_ | | | | '_ \ / __| __| |/ _ \| '_ \/ __|
    // |  _  |  __/ | |_) |  __/ |    |  _|| |_| | | | | (__| |_| | (_) | | | \__ \
    // |_| |_|\___|_| .__/ \___|_|    |_|   \__,_|_| |_|\___|\__|_|\___/|_| |_|___/
    //              |_|

    /**
     * Fill a mocked dao with relations
     *
     * @param access mocked object
     * @param ids    list of RECORD:LIBRARY ids that relations should be
     *               referring to
     * @throws SQLException when error
     */
    private static void fillMockRelations(RawRepoDAO access, String... ids) throws SQLException, RawRepoException {
        HashSet<String> filter = new HashSet<>();
        Collections.addAll(filter, ids);
        HashMap<String, List<String>> from = new HashMap<>();
        HashMap<String, List<String>> to = new HashMap<>();
        for (String id : ids) {
            from.put(id, new ArrayList<>());
            to.put(id, new ArrayList<>());
        }
        for (String relation : RELATIONS) {
            int comma = relation.indexOf(',');
            String l = relation.substring(0, comma);
            String r = relation.substring(comma + 1);
            if (filter.contains(l) && filter.contains(r)) {
                from.get(l).add(r);
                to.get(r).add(l);
            }
        }
        for (String id : filter) {
            RecordId recordId = recordIdFromString(id);
            HashSet<RecordId> parents = new HashSet<>();
            HashSet<RecordId> siblingsFromMe = new HashSet<>();
            for (String target : from.get(id)) {
                RecordId rec = recordIdFromString(target);
                if (rec.getBibliographicRecordId().equals(recordId.getBibliographicRecordId())) {
                    siblingsFromMe.add(rec);
                } else {
                    parents.add(rec);
                }
            }
            when(access.getRelationsParents(recordId)).thenReturn(parents);
            when(access.getRelationsSiblingsFromMe(recordId)).thenReturn(siblingsFromMe);
            when(access.recordExists(recordId.getBibliographicRecordId(), recordId.getAgencyId())).thenReturn(Boolean.TRUE);
            when(access.recordExistsMaybeDeleted(recordId.getBibliographicRecordId(), recordId.getAgencyId())).thenReturn(Boolean.TRUE);
        }
        HashMap<String, HashSet<Integer>> allAgenciesFor = new HashMap<>();
        for (String record : filter) {
            RecordId rec = recordIdFromString(record);
            if (!allAgenciesFor.containsKey(rec.getBibliographicRecordId())) {
                allAgenciesFor.put(rec.getBibliographicRecordId(), new HashSet<>());
            }
            allAgenciesFor.get(rec.getBibliographicRecordId()).add(rec.getAgencyId());
        }
        for (String key : allAgenciesFor.keySet()) {
            when(access.allAgenciesForBibliographicRecordId(key)).thenReturn(allAgenciesFor.get(key));
        }

        doAnswer((Answer<Record>) invocation -> {
            Object[] arguments = invocation.getArguments();
            String content = (arguments[0]) + ":" + (arguments[1]);
            System.out.println("content = " + content);
            Record recordFromContent = recordFromContent(content);
            System.out.println("recordFromContent = " + recordFromContent);
            return recordFromContent;
        }).when(access).fetchRecord(anyString(), anyInt());
    }

    private static int trackingIdCounter = 0;

    private static Record recordFromContent(final String content) {
        String[] split = content.split(":", 2);
        final String id = split[0];
        final int agencyId = Integer.parseInt(split[1]);
        System.out.println("agencyId = " + agencyId);
        return new Record() {
            boolean deleted = false;
            boolean enriched = false;
            String mimeType = agencyId == 870970 ? MarcXChangeMimeType.MARCXCHANGE : MarcXChangeMimeType.ENRICHMENT;
            byte[] c = content.getBytes();
            final String trackingId = "Track-" + (++trackingIdCounter);

            @Override
            public byte[] getContent() {
                return c;
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
                c = content;
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

    private static void recordCollectionIs(Map<String, Record> collection, String... elems) {
        Set<String> set = new HashSet<>(collection.size());
        for (Record record : collection.values()) {
            System.out.println("record = " + record);
            set.add(new String(record.getContent()));
        }
        collectionIs(set, elems);
    }

    /**
     * Parse a string to a recordid
     *
     * @param target ID:LIBRARY
     * @return recordid
     * @throws NumberFormatException when error
     */
    private static RecordId recordIdFromString(String target) throws NumberFormatException {
        int colon = target.indexOf(':');
        String l = target.substring(0, colon);
        String r = target.substring(colon + 1);
        return new RecordId(l, Integer.parseInt(r));
    }

    /**
     * Raise an (descriptive) exception if a collection of strings doesn't match
     * supplied list
     *
     * @param col   collection
     * @param elems string elements collection should consist of
     */
    private static void collectionIs(Collection<String> col, String... elems) {
        HashSet<String> missing = new HashSet<>();
        Collections.addAll(missing, elems);
        HashSet<String> extra = new HashSet<>(col);
        extra.removeAll(missing);
        missing.removeAll(col);
        if (!extra.isEmpty() || !missing.isEmpty()) {
            throw new RuntimeException("missing:" + missing.toString() + ", extra=" + extra.toString());
        }
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
            "H:870970,F:870970",
            "I:870975,D:870970"};

}
