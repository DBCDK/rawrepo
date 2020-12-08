/*
 * dbc-rawrepo-maintain
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-maintain.
 *
 * dbc-rawrepo-maintain is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-maintain is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-maintain.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.maintain;

import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author DBC {@literal <dbc.dk>}
 */
class QueueRecordsIT extends RawRepoTester {

    @Test
    void testQueueSingle() throws Exception {
        QueueRecords mock = makeQueueRecords();

        assertThat("Queue is empty", count("queue"), is(0));
        mock.queueRecord(191919, "40398910", "test");
        assertThat("Queue size", count("queue"), is(2));
    }

    @Test
    void testQueueWithEnrichment() throws Exception {
        QueueRecords mock = makeQueueRecords();

        assertThat("Queue is empty", count("queue"), is(0));
        mock.queueRecord(870970, "40398910", "test");
        assertThat("Queue size", count("queue"), is(3));
    }

    @Test
    void testQueueSection() throws Exception {
        QueueRecords mock = makeQueueRecords();

        assertThat("Queue is empty", count("queue"), is(0));
        mock.queueRecord(191919, "40254641", "test");
        assertThat("Queue size", count("queue"), is(5));
    }

    @Test
    void testQueueEnrichmentSection() throws Exception {
        QueueRecords mock = makeQueueRecords();

        assertThat("Queue is empty", count("queue"), is(0));
        mock.queueRecord(870970, "40254641", "test");
        assertThat("Queue size", count("queue"), is(9));
    }

    private QueueRecords makeQueueRecords() throws RawRepoException, SQLException {
        QueueRecords mock = mock(QueueRecords.class);
        when(mock.getConnection()).thenReturn(pg.getConnection());
        when(mock.getDao()).thenAnswer((InvocationOnMock invocation) -> RawRepoDAO.builder(pg.getConnection()).build());
        doCallRealMethod().when(mock).queueRecord(anyInt(), anyString(), anyString());
        return mock;
    }
}
