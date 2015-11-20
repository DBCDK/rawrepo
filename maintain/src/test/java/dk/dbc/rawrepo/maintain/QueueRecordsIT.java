/*
 * Copyright (C) 2015 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-maintain
 *
 * dbc-rawrepo-maintain is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-maintain is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.maintain;

import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import java.sql.SQLException;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
public class QueueRecordsIT extends RawRepoTester {

    @Test
    public void testQueueSingle() throws Exception {
        System.out.println("testQueueSingle");

        QueueRecords mock = makeQueueRecords();

        assertEquals("Queue is empty", 0, count("queue"));
        mock.queueRecord(870970, "40398910", "test");
        assertEquals("Queue size", 2, count("queue"));
    }

    @Test
    public void testQueueWithEnrichment() throws Exception {
        System.out.println("testQueueWithEnrichment");

        QueueRecords mock = makeQueueRecords();

        assertEquals("Queue is empty", 0, count("queue"));
        mock.queueRecord(191919, "40398910", "test");
        assertEquals("Queue size", 3, count("queue"));
    }

    @Test
    public void testQueueSection() throws Exception {
        System.out.println("testQueueSection");

        QueueRecords mock = makeQueueRecords();

        assertEquals("Queue is empty", 0, count("queue"));
        mock.queueRecord(870970, "40254641", "test");
        assertEquals("Queue size", 5, count("queue"));
    }

    @Test
    public void testQueueEnrichmentSection() throws Exception {
        System.out.println("testQueueEnrichmentSection");

        QueueRecords mock = makeQueueRecords();

        assertEquals("Queue is empty", 0, count("queue"));
        mock.queueRecord(191919, "40254641", "test");
        assertEquals("Queue size", 9, count("queue"));
    }

    private QueueRecords makeQueueRecords() throws RawRepoException, SQLException {
        QueueRecords mock = mock(QueueRecords.class);
        when(mock.getConnection()).thenReturn(pg.connection);
        when(mock.getDao()).thenAnswer((InvocationOnMock invocation) -> RawRepoDAO.builder(pg.connection).build());
        doCallRealMethod().when(mock).queueRecord(anyInt(), anyString(), anyString());
        return mock;
    }
}
