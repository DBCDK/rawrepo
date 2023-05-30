package dk.dbc.rawrepo.maintain;

import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.maintain.transport.StandardResponse;
import dk.dbc.vipcore.exception.VipCoreException;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class QueueRecords extends RawRepoWorker {

    private static final Logger log = LoggerFactory.getLogger(QueueRecords.class);

    public QueueRecords(DataSource dataSource, VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector) {
        super(dataSource, vipCoreLibraryRulesConnector);
    }

    public HashMap<String, ArrayList<String>> getValues(HashMap<String, List<String>> valuesSet, String leaving) {
        final HashMap<String, ArrayList<String>> values = new HashMap<>();

        try {
            values.put("provider", getProviders());
        } catch (SQLException ex) {
            log.error("Error getting values: " + ex.getMessage());
        }
        return values;
    }

    public Object queueRecords(Integer agencyId, List<String> ids, String provider, String trackingId) {
        log.debug("agencyId = " + agencyId +
                "; ids = " + ids +
                "; provider = " + provider +
                "; trackingId = " + trackingId);
        final ArrayList<StandardResponse.Result.Diag> diags = new ArrayList<>();
        int success = 0;
        int failed = 0;
        try {
            final Connection connection = getConnection();
            for (String id : ids) {
                connection.setAutoCommit(false);
                try {
                    queueRecord(agencyId, id, provider);
                    connection.commit();
                    success++;
                } catch (RawRepoException | VipCoreException ex) {
                    failed++;
                    diags.add(new StandardResponse.Result.Diag("Record: " + id, ex.getMessage()));
                    final Throwable cause = ex.getCause();
                    if (cause != null) {
                        log.warn("Record remove error: " + ex.getMessage());
                    }
                    if (!connection.getAutoCommit()) {
                        try {
                            connection.rollback();
                        } catch (SQLException ex1) {
                            log.warn("Cannot roll back " + ex1.getMessage());
                        }
                    }
                }
            }
            StandardResponse.Result.Status status = StandardResponse.Result.Status.SUCCESS;
            final StringBuilder message = new StringBuilder();
            message.append("Done!");
            message.append("\n  * Successfully queued: ").append(success).append(" records.");
            if (failed != 0) {
                status = StandardResponse.Result.Status.PARTIAL;
                message.append("\n  * Failed to queue: ").append(failed).append(" records.");
            }

            return new StandardResponse.Result(status, message.toString(), diags);
        } catch (SQLException ex) {
            log.error("Error getting database connection: " + ex.getMessage());
            return new StandardResponse.Result(StandardResponse.Result.Status.FAILURE, "Error getting database connection");
        }
    }

    void queueRecord(Integer agencyId, String bibliographicRecordId, String provider) throws RawRepoException, VipCoreException {
        final RawRepoDAO dao = getDao();

        if (!dao.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)) {
            throw new RawRepoException("Record doesn't exist");
        }

        dao.changedRecord(provider, new RecordId(bibliographicRecordId, agencyId));
    }
}
