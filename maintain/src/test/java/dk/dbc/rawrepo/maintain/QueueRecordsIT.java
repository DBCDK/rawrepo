package dk.dbc.rawrepo.maintain;

import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.vipcore.exception.VipCoreException;
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

    private QueueRecords makeQueueRecords() throws RawRepoException, SQLException, VipCoreException {
        QueueRecords mock = mock(QueueRecords.class);
        when(mock.getConnection()).thenReturn(pg.getConnection());
        when(mock.getDao()).thenAnswer((InvocationOnMock invocation) -> RawRepoDAO.builder(pg.getConnection()).build());
        doCallRealMethod().when(mock).queueRecord(anyInt(), anyString(), anyString());
        return mock;
    }
}
