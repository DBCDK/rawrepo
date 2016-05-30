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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.*;

import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import org.junit.Assert;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.doCallRealMethod;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
@RunWith(MockitoJUnitRunner.class)
public class RawRepoDAOTest {

    public RawRepoDAOTest() {
    }

    @Test
    public void testEnrichmentTrail() throws RawRepoException, MarcXMergerException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            doCallRealMethod().when(access).fetchMergedRecord(anyString(), anyInt(), (MarcXMerger) anyObject(), anyBoolean());
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
                    public byte[] merge(byte[] common, byte[] local, boolean includeAllFields) throws MarcXMergerException {
                        return common;
                    }

                    @Override
                    public boolean canMerge(String originalMimeType, String enrichmentMimeType) {
                        return true;
                    }

                };
            Assert.assertEquals("870970", access.fetchMergedRecord("D", 870970, marcXMerger, true).getEnrichmentTrail());
            Assert.assertEquals("870970,1", access.fetchMergedRecord("D", 1, marcXMerger, true).getEnrichmentTrail());

        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

    }

    @Test
    public void testQueueEntityWithout() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "A:870970");
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("A:870970"), "text/plain");
            verify(access, times(1)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol, "A:870970:Y:Y");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueEntityWith() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "A:870970", "A:1", "A:2");
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("A:870970"), "text/plain");
            verify(access, times(3)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol, "A:870970:Y:Y", "A:1:N:Y", "A:2:N:Y");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueEntityLocal() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "A:870970", "A:1", "A:2");
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("A:1"), "text/plain");
            verify(access, times(1)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol, "A:1:Y:Y");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueBindWithout() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", // HEAD
                              "C:870970", // SECTION
                              "D:870970", // BIND
                              "E:870970", // BIND
                              "F:870970", // SECTION
                              "G:870970", // BIND
                              "H:870970");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("D:870970"), "text/plain");
            verify(access, times(1)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol, "D:870970:Y:Y");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueBindWith() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", // HEAD
                              "C:870970", // SECTION
                              "D:870970", "D:1", "D:2", // BIND
                              "E:870970", "E:1", "E:2", // BIND
                              "F:870970", // SECTION
                              "G:870970", "G:1", "G:2", // BIND
                              "H:870970", "H:1", "H:2");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("D:870970"), "text/plain");
            verify(access, times(3)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol, "D:870970:Y:Y", "D:1:N:Y", "D:2:N:Y");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueBindLocal() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", // HEAD
                              "C:870970", // SECTION
                              "D:870970", "D:1", "D:2", // BIND
                              "E:870970", "E:1", "E:2", // BIND
                              "F:870970", // SECTION
                              "G:870970", "G:1", "G:2", // BIND
                              "H:870970", "H:1", "H:2");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("D:2"), "text/plain");
            verify(access, times(1)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol, "D:2:Y:Y");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueBindWithSectionLocal() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", // HEAD
                              "C:870970", "C:1", "C:2", // SECTION
                              "D:870970", // BIND
                              "E:870970", // BIND
                              "F:870970", "F:1", "F:2", // SECTION
                              "G:870970", // BIND
                              "H:870970");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("D:870970"), "text/plain");
            verify(access, times(3)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol, "D:870970:Y:Y", "D:1:N:Y", "D:2:N:Y");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueBindWithHeadLocal() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", "B:1", "B:2", // HEAD
                              "C:870970", // SECTION
                              "D:870970", // BIND
                              "E:870970", // BIND
                              "F:870970", // SECTION
                              "G:870970", // BIND
                              "H:870970");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("D:870970"), "text/plain");
            verify(access, times(3)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol, "D:870970:Y:Y", "D:1:N:Y", "D:2:N:Y");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueSectionWithout() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", // HEAD
                              "C:870970",// SECTION
                              "D:870970", // BIND
                              "E:870970", // BIND
                              "F:870970",// SECTION
                              "G:870970", // BIND
                              "H:870970");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("C:870970"), "text/plain");
            verify(access, times(3)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol,
                         "C:870970:Y:N", //  SECTION
                         "D:870970:N:Y", //  BIND
                         "E:870970:N:Y"); // BIND
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueSectionWith() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", // HEAD
                              "C:870970", "C:1", "C:2",// SECTION
                              "D:870970", // BIND
                              "E:870970", // BIND
                              "F:870970", "F:1", "F:2",// SECTION
                              "G:870970", // BIND
                              "H:870970");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("C:870970"), "text/plain");
            verify(access, times(9)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol,
                         "C:870970:Y:N", "C:1:N:N", "C:2:N:N", //  SECTION
                         "D:870970:N:Y", "D:1:N:Y", "D:2:N:Y", //  BIND
                         "E:870970:N:Y", "E:1:N:Y", "E:2:N:Y"); // BIND
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueSectionLocal() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", // HEAD
                              "C:870970", "C:1", "C:2",// SECTION
                              "D:870970", // BIND
                              "E:870970", // BIND
                              "F:870970", "F:1", "F:2",// SECTION
                              "G:870970", // BIND
                              "H:870970");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("C:1"), "text/plain");
            verify(access, times(3)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol,
                         "C:1:Y:N", //  SECTION
                         "D:1:N:Y", //  BIND
                         "E:1:N:Y"); // BIND
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueSectionWithBindLocal() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", // HEAD
                              "C:870970", "C:1", "C:2",// SECTION
                              "D:870970", // BIND
                              "E:870970", // BIND
                              "F:870970", "F:1", "F:2",// SECTION
                              "G:870970", // BIND
                              "H:870970");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("C:870970"), "text/plain");
            verify(access, times(9)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol,
                         "C:870970:Y:N", "C:1:N:N", "C:2:N:N", //  SECTION
                         "D:870970:N:Y", "D:1:N:Y", "D:2:N:Y", //  BIND
                         "E:870970:N:Y", "E:1:N:Y", "E:2:N:Y"); // BIND
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueSectionWithHeadLocal() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", "B:1", "B:2", // HEAD
                              "C:870970",// SECTION
                              "D:870970", // BIND
                              "E:870970", // BIND
                              "F:870970",// SECTION
                              "G:870970", // BIND
                              "H:870970");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("C:870970"), "text/plain");
            verify(access, times(9)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol,
                         "C:870970:Y:N", "C:1:N:N", "C:2:N:N", //  SECTION
                         "D:870970:N:Y", "D:1:N:Y", "D:2:N:Y", //  BIND
                         "E:870970:N:Y", "E:1:N:Y", "E:2:N:Y"); // BIND
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueHeadWithout() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", // HEAD
                              "C:870970", // SECTION
                              "D:870970", // BIND
                              "E:870970", // BIND
                              "F:870970", // SECTION
                              "G:870970", // BIND
                              "H:870970");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("B:870970"), "text/plain");
            verify(access, times(7)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol,
                         "B:870970:Y:N", //  HEAD
                         "C:870970:N:N", //  SECTION
                         "D:870970:N:Y", //  BIND
                         "E:870970:N:Y", //  BIND
                         "F:870970:N:N", //  SECTION
                         "G:870970:N:Y", //  BIND
                         "H:870970:N:Y"); // BIND
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueHeadWith() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", "B:1", "B:2", // HEAD
                              "C:870970", // SECTION
                              "D:870970", // BIND
                              "E:870970", // BIND
                              "F:870970", // SECTION
                              "G:870970", // BIND
                              "H:870970");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("B:870970"), "text/plain");
            verify(access, times(21)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol,
                         "B:870970:Y:N", "B:1:N:N", "B:2:N:N", //  HEAD
                         "C:870970:N:N", "C:1:N:N", "C:2:N:N", //  SECTION
                         "D:870970:N:Y", "D:1:N:Y", "D:2:N:Y", //  BIND
                         "E:870970:N:Y", "E:1:N:Y", "E:2:N:Y", //  BIND
                         "F:870970:N:N", "F:1:N:N", "F:2:N:N", //  SECTION
                         "G:870970:N:Y", "G:1:N:Y", "G:2:N:Y", //  BIND
                         "H:870970:N:Y", "H:1:N:Y", "H:2:N:Y"); // BIND
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueHeadLocal() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", "B:1", "B:2", // HEAD
                              "C:870970", // SECTION
                              "D:870970", // BIND
                              "E:870970", // BIND
                              "F:870970", // SECTION
                              "G:870970", // BIND
                              "H:870970");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("B:1"), "text/plain");
            verify(access, times(7)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol,
                         "B:1:Y:N", //  HEAD
                         "C:1:N:N", //  SECTION
                         "D:1:N:Y", //  BIND
                         "E:1:N:Y", //  BIND
                         "F:1:N:N", //  SECTION
                         "G:1:N:Y", //  BIND
                         "H:1:N:Y"); // BIND
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueHeadWithBindLocal() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", // HEAD
                              "C:870970", // SECTION
                              "D:870970", "D:1", // BIND
                              "E:870970", "E:2", // BIND
                              "F:870970", // SECTION
                              "G:870970", "G:1", "G:2", // BIND
                              "H:870970");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("B:870970"), "text/plain");
            verify(access, times(11)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol,
                         "B:870970:Y:N", //  HEAD
                         "C:870970:N:N", //  SECTION
                         "D:870970:N:Y", "D:1:N:Y", //  BIND
                         "E:870970:N:Y", "E:2:N:Y", //  BIND
                         "F:870970:N:N", //  SECTION
                         "G:870970:N:Y", "G:1:N:Y", "G:2:N:Y", //  BIND
                         "H:870970:N:Y"); // BIND
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueHeadWithSectionLocal() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", // HEAD
                              "C:870970", "C:1", // SECTION
                              "D:870970", // BIND
                              "E:870970", // BIND
                              "F:870970", "F:1", "F:2", // SECTION
                              "G:870970", // BIND
                              "H:870970");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("B:870970"), "text/plain");
            verify(access, times(16)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol,
                         "B:870970:Y:N", //  HEAD
                         "C:870970:N:N", "C:1:N:N", //  SECTION
                         "D:870970:N:Y", "D:1:N:Y", //  BIND
                         "E:870970:N:Y", "E:1:N:Y", //  BIND
                         "F:870970:N:N", "F:1:N:N", "F:2:N:N", //  SECTION
                         "G:870970:N:Y", "G:1:N:Y", "G:2:N:Y", //  BIND
                         "H:870970:N:Y", "H:1:N:Y", "H:2:N:Y"); // BIND
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testQueueHeadWithComplexLocal() throws RawRepoException {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();
            doCallRealMethod().when(access).changedRecord(anyString(), any(RecordId.class), anyString());
            fillMockRelations(access,
                              "B:870970", // HEAD
                              "C:870970", "C:1", // SECTION
                              "D:870970", // BIND
                              "E:870970", "E:2", // BIND
                              "F:870970", "F:2", // SECTION
                              "G:870970", "G:1", // BIND
                              "H:870970");// BIND
            Collection<String> eCol = mockCollectEnqueue(access);
            access.changedRecord("foo", recordIdFromString("B:870970"), "text/plain");
            verify(access, times(15)).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
            collectionIs(eCol,
                         "B:870970:Y:N", //  HEAD
                         "C:870970:N:N", "C:1:N:N", //  SECTION
                         "D:870970:N:Y", "D:1:N:Y", //  BIND
                         "E:870970:N:Y", "E:1:N:Y", "E:2:N:Y", //  BIND
                         "F:870970:N:N", "F:2:N:N", //  SECTION
                         "G:870970:N:Y", "G:1:N:Y", "G:2:N:Y", //  BIND
                         "H:870970:N:Y", "H:2:N:Y"); // BIND
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testFetchRecordCollection() throws RawRepoException, MarcXMergerException, Exception {
        try {
            RawRepoDAO access = mock(RawRepoDAO.class);
            access.agencySearchOrder = new AgencySearchOrderFallback();

            access.relationHints = mock(RelationHintsOpenAgency.class);
            doAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    return true;
                }
            }).when(access.relationHints).usesCommonAgency(anyInt());
            doAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    return false;
                }
            }).when(access.relationHints).usesCommonAgency(2);

            doAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    return Arrays.asList(870970);
                }
            }).when(access.relationHints).get(anyInt());

            doCallRealMethod().when(access).fetchRecordCollection(anyString(), anyInt(), (MarcXMerger) anyObject());
            doCallRealMethod().when(access).agencyFor(anyString(), anyInt(), anyBoolean());
            doCallRealMethod().when(access).fetchMergedRecord(anyString(), anyInt(), (MarcXMerger) anyObject(), anyBoolean());
            doCallRealMethod().when(access).findParentRelationAgency(anyString(), anyInt());
            fillMockRelations(access,
                              "A:870970", "A:1", "A:2",
                              "B:870970", // HEAD
                              "C:870970", "C:1", // SECTION
                              "D:870970", // BIND
                              "E:870970", "E:2", // BIND
                              "F:870970", "F:2", // SECTION
                              "G:870970", "G:1", // BIND
                              "H:870970");// BIND
            System.out.println("access = " + access);
            MarcXMerger merger = new MarcXMerger() {

                    @Override
                    public byte[] merge(byte[] common, byte[] local, boolean isFinal) throws MarcXMergerException {
                        return local;
                    }

                };
            recordCollectionIs(access.fetchRecordCollection("A", 870970, merger), "A:870970"); // ENTITY
            recordCollectionIs(access.fetchRecordCollection("A", 1, merger), "A:1"); // ENTITY LOCAL
            recordCollectionIs(access.fetchRecordCollection("D", 870970, merger), "D:870970", "C:870970", "B:870970"); // BIND
            recordCollectionIs(access.fetchRecordCollection("D", 1, merger), "D:870970", "C:1", "B:870970"); // BIND LOCAL SECTION
            recordCollectionIs(access.fetchRecordCollection("G", 1, merger), "G:1", "F:870970", "B:870970"); // BIND LOCAL BIND
            recordCollectionIs(access.fetchRecordCollection("F", 2, merger), "F:2"); // NO COMMON AGENCY

        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void parentRelationAgency() throws Exception {
        RawRepoDAO mock = mock(RawRepoDAO.class);
        mock.relationHints = mock(RelationHints.class);
        doCallRealMethod().when(mock).findParentRelationAgency(anyString(), anyInt());
        when(mock.relationHints.get(123456)).thenReturn(Arrays.asList(300000, 870970));
        when(mock.relationHints.usesCommonAgency(123456)).thenReturn(Boolean.TRUE);
        when(mock.relationHints.get(654321)).thenReturn(Arrays.asList());
        when(mock.relationHints.usesCommonAgency(654321)).thenReturn(Boolean.FALSE);
        when(mock.recordExists(anyString(), anyInt())).thenReturn(Boolean.FALSE);
        when(mock.recordExists("PRIVATE", 123456)).thenReturn(Boolean.TRUE);
        when(mock.recordExists("PRIVATE", 654321)).thenReturn(Boolean.TRUE);
        when(mock.recordExists("COMMON", 870970)).thenReturn(Boolean.TRUE);
        when(mock.recordExists("COMMON", 123456)).thenReturn(Boolean.TRUE);
        when(mock.recordExists("COMMON", 654321)).thenReturn(Boolean.TRUE);
        int parentRelationAgency;
        parentRelationAgency = mock.findParentRelationAgency("PRIVATE", 123456);
        Assert.assertEquals(123456, parentRelationAgency);
        parentRelationAgency = mock.findParentRelationAgency("COMMON", 123456);
        Assert.assertEquals(870970, parentRelationAgency);
        parentRelationAgency = mock.findParentRelationAgency("PRIVATE", 654321);
        Assert.assertEquals(654321, parentRelationAgency);
        parentRelationAgency = mock.findParentRelationAgency("COMMON", 654321);
        Assert.assertEquals(654321, parentRelationAgency);
        try {
            mock.findParentRelationAgency("NA", 123456);
            Assert.fail("Expected RawRepoExceptionRecordNotFound");
        } catch (RawRepoExceptionRecordNotFound ex) {
        }
    }

    @Test
    public void siblingRelationAgency() throws Exception {
        RawRepoDAO mock = mock(RawRepoDAO.class);
        mock.relationHints = mock(RelationHints.class);
        doCallRealMethod().when(mock).findSiblingRelationAgency(anyString(), anyInt());
        when(mock.relationHints.get(123456)).thenReturn(Arrays.asList(300000, 870970));
        when(mock.relationHints.usesCommonAgency(123456)).thenReturn(Boolean.TRUE);
        when(mock.recordExists(anyString(), anyInt())).thenReturn(Boolean.FALSE);
        when(mock.recordExists("PRIVATE", 123456)).thenReturn(Boolean.TRUE);
        when(mock.recordExists("COMMON", 870970)).thenReturn(Boolean.TRUE);
        when(mock.recordExists("COMMON", 123456)).thenReturn(Boolean.TRUE);
        when(mock.recordExists("INTERM", 870970)).thenReturn(Boolean.TRUE);
        when(mock.recordExists("INTERM", 300000)).thenReturn(Boolean.TRUE);
        when(mock.recordExists("INTERM", 123456)).thenReturn(Boolean.TRUE);
        int siblingRelationAgency;
        siblingRelationAgency = mock.findSiblingRelationAgency("COMMON", 123456);
        System.out.println("parentRelationAgency = " + siblingRelationAgency);
        Assert.assertEquals(870970, siblingRelationAgency);
        siblingRelationAgency = mock.findSiblingRelationAgency("INTERM", 123456);
        Assert.assertEquals(300000, siblingRelationAgency);
        try {
            mock.findSiblingRelationAgency("PRIVATE", 123456);
            Assert.fail("Expected RawRepoExceptionRecordNotFound");
        } catch (RawRepoExceptionRecordNotFound ex) {
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
     *               refering to
     * @throws SQLException
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
            HashSet<RecordId> all = new HashSet<>();
            HashSet<RecordId> parents = new HashSet<>();
            HashSet<RecordId> siblingsToMe = new HashSet<>();
            HashSet<RecordId> siblingsFromMe = new HashSet<>();
            HashSet<RecordId> children = new HashSet<>();
            for (String target : from.get(id)) {
                RecordId rec = recordIdFromString(target);
                all.add(rec);
                if (rec.getBibliographicRecordId().equals(recordId.getBibliographicRecordId())) {
                    siblingsFromMe.add(rec);
                } else {
                    parents.add(rec);
                }
            }
            for (String target : to.get(id)) {
                RecordId rec = recordIdFromString(target);
                if (rec.getBibliographicRecordId().equals(recordId.getBibliographicRecordId())) {
                    siblingsToMe.add(rec);
                } else {
                    children.add(rec);
                }
            }

            when(access.getRelationsParents(recordId)).thenReturn(parents);
            when(access.getRelationsFrom(recordId)).thenReturn(all);
            when(access.getRelationsChildren(recordId)).thenReturn(children);
            when(access.getRelationsSiblingsToMe(recordId)).thenReturn(siblingsToMe);
            when(access.getRelationsSiblingsFromMe(recordId)).thenReturn(siblingsFromMe);
            when(access.recordExists(recordId.getBibliographicRecordId(), recordId.getAgencyId())).thenReturn(Boolean.TRUE);
            when(access.recordExistsMabyDeleted(recordId.getBibliographicRecordId(), recordId.getAgencyId())).thenReturn(Boolean.TRUE);
        }
        HashMap<String, HashSet<Integer>> allAgenciesFor = new HashMap<>();
        for (String record : filter) {
            RecordId rec = recordIdFromString(record);
            if (!allAgenciesFor.containsKey(rec.getBibliographicRecordId())) {
                allAgenciesFor.put(rec.getBibliographicRecordId(), new HashSet<Integer>());
            }
            allAgenciesFor.get(rec.getBibliographicRecordId()).add(rec.getAgencyId());
        }
        for (String key : allAgenciesFor.keySet()) {
            when(access.allAgenciesForBibliographicRecordId(key)).thenReturn(allAgenciesFor.get(key));
        }

        doAnswer(new Answer<Record>() {

            @Override
            public Record answer(InvocationOnMock invocation) throws Throwable {
                Object[] arguments = invocation.getArguments();
                String content = ( (String) arguments[0] ) + ":" + ( (int) arguments[1] );
                System.out.println("content = " + content);
                Record recordFromContent = recordFromContent(content);
                System.out.println("recordFromContent = " + recordFromContent);
                return recordFromContent;
            }
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
            String trackingId = "Track-" + ( ++trackingIdCounter );

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
            public Date getCreated() {
                return new Date();
            }

            @Override
            public void setCreated(Date created) {
            }

            @Override
            public RecordId getId() {
                return new RecordId(id, agencyId);
            }

            @Override
            public Date getModified() {
                return new Date();
            }

            @Override
            public void setModified(Date modified) {
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
     * Set up a collector for .enqueue(...) on a mocked object
     *
     * @param access mocked object
     * @return collection that will contain all calls to enqueue
     * @throws SQLException
     */
    private static Collection<String> mockCollectEnqueue(RawRepoDAO access) throws SQLException, RawRepoException {
        final HashSet<String> collection = new HashSet();
        doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] arguments = invocation.getArguments();
                collection.add(( (RecordId) arguments[0] ).getBibliographicRecordId() + ":" + ( (RecordId) arguments[0] ).getAgencyId() +
                               ( ( (Boolean) arguments[3] ) ? ":Y" : ":N" ) +
                               ( ( (Boolean) arguments[4] ) ? ":Y" : ":N" ));
                return null;
            }
        }).when(access).enqueue((RecordId) anyObject(), anyString(), anyString(), anyBoolean(), anyBoolean());
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
        int colon = target.indexOf(':');
        String l = target.substring(0, colon);
        String r = target.substring(colon + 1);
        final RecordId recordId = new RecordId(l, Integer.parseInt(r));
        return recordId;
    }

    /**
     * Raise an (descriptive) exception if a collection of strings doesn't match
     * supplied list
     *
     * @param col   collection
     * @param elems string elements collection should consist of
     */
    private static void collectionIs(Collection<String> col, String... elems) {
        HashSet<String> missing = new HashSet();
        Collections.addAll(missing, elems);
        HashSet<String> extra = new HashSet(col);
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
        "H:870970,F:870970"};

}
