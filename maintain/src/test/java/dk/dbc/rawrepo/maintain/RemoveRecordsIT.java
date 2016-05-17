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
public class RemoveRecordsIT extends RawRepoTester {

    @Test
    public void testRemove() throws Exception {
        System.out.println("testRemove");

        RemoveRecords mock = makeRemoveRecords();

        int records = count("records WHERE NOT deleted");
        assertEquals("Empty Queue:", 0, count("queue"));

        mock.removeRecord(191919, "40254692", "test", "track");

        assertEquals("Not deleted records:", records - 1, count("records WHERE NOT deleted"));
        assertEquals("Queue size:", 2, count("queue"));
    }

    @Test
    public void testRemoveParentEnrichment() throws Exception {
        System.out.println("testRemoveParentEnrichment");

        RemoveRecords mock = makeRemoveRecords();

        int records = count("records WHERE NOT deleted");
        assertEquals("Empty Queue:", 0, count("queue"));

        mock.removeRecord(191919, "40254641", "test", "track");

        assertEquals("Not deleted records:", records - 1, count("records WHERE NOT deleted"));
        assertEquals("Queue size:", 5, count("queue"));
    }

    @Test(expected = RawRepoException.class)
    public void testRemoveDouble() throws Exception {
        System.out.println("testRemoveDouble");

        RemoveRecords mock = makeRemoveRecords();

        int records = count("records WHERE NOT deleted");
        assertEquals("Empty Queue:", 0, count("queue"));

        mock.removeRecord(191919, "40254692", "test", "track");

        assertEquals("Not deleted records:", records - 1, count("records WHERE NOT deleted"));
        assertEquals("Queue size:", 2, count("queue"));

        mock.removeRecord(191919, "40254692", "test", "track");

    }

    @Test(expected = RawRepoException.class)
    public void testRemoveNA() throws Exception {
        System.out.println("testRemoveNA");

        RemoveRecords mock = makeRemoveRecords();

        mock.removeRecord(191919, "NO SUCH RECORD", "test", "track");
    }

    @Test(expected = RawRepoException.class)
    public void testRemoveSibling() throws Exception {
        System.out.println("testRemoveSibling");

        RemoveRecords mock = makeRemoveRecords();

        mock.removeRecord(870970, "40398910", "test", "track");
    }

    @Test(expected = RawRepoException.class)
    public void testRemoveParent() throws Exception {
        System.out.println("testRemoveParent");

        RemoveRecords mock = makeRemoveRecords();

        mock.removeRecord(870970, "40398899", "test", "track");
    }

    private RemoveRecords makeRemoveRecords() throws RawRepoException, SQLException {
        RemoveRecords mock = mock(RemoveRecords.class);
        when(mock.getConnection()).thenReturn(pg.connection);
        when(mock.getDao()).thenAnswer((InvocationOnMock invocation) -> RawRepoDAO.builder(pg.connection).build());
        doCallRealMethod().when(mock).removeRecord(anyInt(), anyString(), anyString(), anyString());
        return mock;
    }
}
