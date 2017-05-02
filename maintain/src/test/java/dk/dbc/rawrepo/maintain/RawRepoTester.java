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

import dk.dbc.commons.testutils.postgres.connection.PostgresITConnection;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.RelationHints;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.FileNotFoundException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import org.junit.Before;

import static org.mockito.Mockito.*;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
class RawRepoTester {

    protected PostgresITConnection pg;
    protected TestQueue testQueue;

    @Before
    public void setUp() throws SQLException, FileNotFoundException {
        pg = new PostgresITConnection("rawrepo");
        testQueue = new TestQueue();

        pg.clearTables("records", "records_archive", "relations", "queueworkers", "queuerules", "messagequeuerules");
        pg.loadTables("records", "records_archive", "relations", "queueworkers", "queuerules", "messagequeuerules");
    }

    @SuppressFBWarnings(value = {"UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
                                 "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE"},
                        justification = "connection initliazed in @Before." +
                                        "arg might contain 'where ...'")
    protected int count(String arg) throws SQLException {
        try (Statement stmt = pg.getConnection().createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery("SELECT COUNT(*) FROM " + arg)) {
                resultSet.next();
                return resultSet.getInt(1);
            }
        }
    }

    @SuppressFBWarnings(value = {"UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR"},
                        justification = "connection initliazed in @Before")
    protected RawRepoDAO getDao() throws RawRepoException, Exception {
        RelationHints relationHints = mock(RelationHints.class);
        when(relationHints.get(anyInt())).thenReturn(Arrays.asList());
        when(relationHints.usesCommonAgency(anyInt())).thenReturn(Boolean.FALSE);

        return RawRepoDAO.builder(pg.getConnection()).relationHints(relationHints).build();
    }

}
