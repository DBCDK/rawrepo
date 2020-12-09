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
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * @author DBC {@literal <dbc.dk>}
 */
class RevertRecordsIT extends RawRepoTester {

    @Test
    void testRevertRecords() throws Exception {
        RevertRecords mock = makeRevertRecords();
        RawRepoDAO dao = getDao();
        Record recordBefore = dao.fetchRecord("H1", 100000);
        long incarnationBefore = getNumber(recordBefore.getContent());
        assertThat("Old incarnation", incarnationBefore, is(3L));

        mock.revertRecord(100000, "H1", ts("2015-01-01 00:30:00.000"), "", "Track");

        Record recordAfter = dao.fetchRecord("H1", 100000);
        long incarnationAfter = getNumber(recordAfter.getContent());
        assertThat("New incarnation", incarnationAfter, is(1L));
    }

    @Test
    void testRevertRecordsWithAuthority() throws Exception {
        RevertRecords mock = makeRevertRecords();
        RawRepoDAO dao = getDao();
        Set<RecordId> existingRelationsFrom = dao.getRelationsFrom(new RecordId("54248229", 870970));
        assertThat("Existing relations count", existingRelationsFrom.size(), is(0));
        mock.revertRecord(870970, "54248229", ts("2019-02-07 04:00:00.000"), "", "Track");

        Set<RecordId> newRelationsFrom = dao.getRelationsFrom(new RecordId("54248229", 870970));
        assertThat("New relations count", newRelationsFrom.size(), is(1));
        assertTrue(newRelationsFrom.contains(new RecordId("68359775", 870979)));
    }

    @Test
    void testRevertRecordsNewest() throws Exception {
        RevertRecords mock = makeRevertRecords();

        Assertions.assertThrows(RawRepoException.class, () -> mock.revertRecord(100000, "H1", ts("2015-06-01 00:30:00.000"), "", "Track"));
    }

    @Test
    void testRevertRecordsAncient() throws Exception {
        RevertRecords mock = makeRevertRecords();

        Assertions.assertThrows(RawRepoException.class, () -> mock.revertRecord(100000, "H1", ts("2010-01-01 12:34:56.789"), "", "Track"));
    }

    @Test
    void testRevertRecordsNotFound() throws Exception {
        RevertRecords mock = makeRevertRecords();

        Assertions.assertThrows(RawRepoException.class, () -> mock.revertRecord(100000, "NONE", ts("2015-01-01 00:30:00.000"), "", "Track"));
    }

    /*
     *       __  __     __
     *      / / / /__  / /___  ___  __________
     *     / /_/ / _ \/ / __ \/ _ \/ ___/ ___/
     *    / __  /  __/ / /_/ /  __/ /  (__  )
     *   /_/ /_/\___/_/ .___/\___/_/  /____/
     *               /_/
     */
    private static final Pattern DIGITS = Pattern.compile("(\\d+)");

    private static long getNumber(byte[] content) {
        Matcher matcher = DIGITS.matcher(new String(content, StandardCharsets.UTF_8));
        if (matcher.find()) {
            return Long.parseLong(matcher.group(), 10);
        }
        throw new IllegalArgumentException("Argument doesn't contain digits.");
    }

    private static long ts(String time) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        return format.parse(time).getTime();
    }

    private RevertRecords makeRevertRecords() throws RawRepoException, SQLException {
        RevertRecords mock = mock(RevertRecords.class);
        when(mock.getConnection()).thenReturn(pg.getConnection());
        when(mock.getDao()).thenAnswer((InvocationOnMock invocation) -> RawRepoDAO.builder(pg.getConnection()).build());
        doCallRealMethod().when(mock).revertRecord(anyInt(), anyString(), anyLong(), anyString(), anyString());
        return mock;
    }

}
