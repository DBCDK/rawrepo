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

import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.QueueJob;
import dk.dbc.rawrepo.RawRepoException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import javax.jms.JMSException;
import javax.sql.DataSource;
import javax.xml.transform.TransformerException;
import org.junit.Test;
import org.w3c.dom.DOMException;
import org.xml.sax.SAXException;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class RemoveRecordsIT extends RawRepoTester {

    @Test
    public void testRemove() throws Exception {
        System.out.println("testRemove");

        RemoveRecords mock = makeRemoveRecords();

        int records = count("records WHERE NOT deleted");
        assertEquals("Empty Queue:", 0, testQueue.size());

        mock.removeRecord(191919, "40254692", "test", "track");

        assertEquals("Not deleted records:", records - 1, count("records WHERE NOT deleted"));
        assertEquals("Queue size:", 2, testQueue.size());
    }

    @Test
    public void testRemoveParentEnrichment() throws Exception {
        System.out.println("testRemoveParentEnrichment");

        RemoveRecords mock = makeRemoveRecords();

        int records = count("records WHERE NOT deleted");
        assertEquals("Empty Queue:", 0, testQueue.size());

        mock.removeRecord(191919, "40254641", "test", "track");

        assertEquals("Not deleted records:", records - 1, count("records WHERE NOT deleted"));
        assertEquals("Queue size:", 5, testQueue.size());
    }

    @Test(expected = RawRepoException.class)
    public void testRemoveDouble() throws Exception {
        System.out.println("testRemoveDouble");

        RemoveRecords mock = makeRemoveRecords();

        int records = count("records WHERE NOT deleted");
        assertEquals("Empty Queue:", 0, testQueue.size());

        mock.removeRecord(191919, "40254692", "test", "track");

        assertEquals("Not deleted records:", records - 1, count("records WHERE NOT deleted"));
        System.out.println("testQueue = " + testQueue);
        assertEquals("Queue size:", 2, testQueue.size());

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

    private RemoveRecords makeRemoveRecords() throws RawRepoException, SQLException, MarcXMergerException, SAXException, TransformerException, DOMException, IOException {
        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(pg.getConnection());

        OpenAgencyServiceFromURL openAgency = mock(OpenAgencyServiceFromURL.class);
        return new RemoveRecords(dataSource, testQueue, openAgency);

    }
}
