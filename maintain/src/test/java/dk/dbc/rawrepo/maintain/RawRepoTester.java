package dk.dbc.rawrepo.maintain;

import dk.dbc.commons.testutils.postgres.connection.PostgresITConnection;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.RelationHintsVipCore;
import dk.dbc.vipcore.exception.VipCoreException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.BeforeEach;

import java.io.FileNotFoundException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author DBC {@literal <dbc.dk>}
 */
class RawRepoTester {

    protected PostgresITConnection pg;

    @BeforeEach
    public void setUp() throws SQLException, FileNotFoundException {
        pg = new PostgresITConnection("rawrepo");

        pg.clearTables("records", "records_archive", "relations", "queueworkers", "queuerules");
        pg.loadTables("records", "records_archive", "relations", "queueworkers", "queuerules");
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
    protected RawRepoDAO getDao() throws RawRepoException, VipCoreException {
        RelationHintsVipCore relationHints = mock(RelationHintsVipCore.class);
        when(relationHints.get(anyInt())).thenReturn(Collections.emptyList());
        when(relationHints.usesCommonAgency(anyInt())).thenReturn(Boolean.FALSE);
        when(relationHints.usesCommonAgency(870970)).thenReturn(Boolean.TRUE);
        when(relationHints.usesCommonAgency(870979)).thenReturn(Boolean.TRUE);

        return RawRepoDAO.builder(pg.getConnection()).relationHints(relationHints).build();
    }

}
