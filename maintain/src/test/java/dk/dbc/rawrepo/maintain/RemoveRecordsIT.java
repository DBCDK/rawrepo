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
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnectorFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.w3c.dom.DOMException;

import javax.sql.DataSource;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class RemoveRecordsIT extends RawRepoTester {

    @Test
    public void testRemove() throws Exception {
        System.out.println("testRemove");

        RemoveRecords mock = makeRemoveRecords();

        int records = count("records WHERE NOT deleted");
        assertThat("Empty Queue:", count("queue"), is(0));

        mock.removeRecord(191919, "40254692", "test", "track");

        assertThat("Not deleted records:", count("records WHERE NOT deleted"), is(records - 1));
        assertThat("Queue size:", count("queue"), is(2));
    }

    @Test
    public void testRemoveParentEnrichment() throws Exception {
        System.out.println("testRemoveParentEnrichment");

        RemoveRecords mock = makeRemoveRecords();

        int records = count("records WHERE NOT deleted");
        assertThat("Empty Queue:", count("queue"), is(0));

        mock.removeRecord(191919, "40254641", "test", "track");

        assertThat("Not deleted records:", count("records WHERE NOT deleted"), is(records - 1));
//        assertThat("Queue size:", count("queue"), is(0));
    }

    @Test
    public void testRemoveDouble() throws Exception {
        System.out.println("testRemoveDouble");

        RemoveRecords mock = makeRemoveRecords();

        int records = count("records WHERE NOT deleted");
        assertThat("Empty Queue:", count("queue"), is(0));

        mock.removeRecord(191919, "40254692", "test", "track");

        assertThat("Not deleted records:", count("records WHERE NOT deleted"), is(records - 1));
        assertThat("Queue size:", count("queue"), is(2));

        Assertions.assertThrows(RawRepoException.class, () -> mock.removeRecord(191919, "40254692", "test", "track"));
    }

    @Test
    public void testRemoveNA() throws Exception {
        System.out.println("testRemoveNA");

        RemoveRecords mock = makeRemoveRecords();

        Assertions.assertThrows(RawRepoException.class, () -> mock.removeRecord(191919, "NO SUCH RECORD", "test", "track"));
    }

    @Test
    public void testRemoveSibling() throws Exception {
        System.out.println("testRemoveSibling");

        RemoveRecords mock = makeRemoveRecords();

        Assertions.assertThrows(RawRepoException.class, () -> mock.removeRecord(870970, "40398910", "test", "track"));
    }

    @Test
    public void testRemoveParent() throws Exception {
        System.out.println("testRemoveParent");

        RemoveRecords mock = makeRemoveRecords();

        Assertions.assertThrows(RawRepoException.class, () -> mock.removeRecord(870970, "40398899", "test", "track"));
    }

    private RemoveRecords makeRemoveRecords() throws SQLException, MarcXMergerException, DOMException {
        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(pg.getConnection());


        VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector = VipCoreLibraryRulesConnectorFactory.create("http://vipcore.iscrum-vip-staging.svc.cloud.dbc.dk/");
        return new RemoveRecords(dataSource, vipCoreLibraryRulesConnector);
    }
}
