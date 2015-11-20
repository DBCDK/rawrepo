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

import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.maintain.transport.StandardResponse;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
public class QueueRecords extends RawRepoWorker {

    private static final Logger log = LoggerFactory.getLogger(QueueRecords.class);

    public QueueRecords(DataSource dataSource, OpenAgencyServiceFromURL openAgency, ExecutorService executorService) {
        super(dataSource, openAgency, executorService);
    }

    public HashMap<String, ArrayList<String>> getValues(HashMap<String, List<String>> valuesSet, String leaving) {
        HashMap<String, ArrayList<String>> values = new HashMap<>();

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
        ArrayList<StandardResponse.Result.Diag> diags = new ArrayList<>();
        int success = 0;
        int failed = 0;
        try {
            Connection connection = getConnection();
            for (String id : ids) {
                connection.setAutoCommit(false);
                try {
                    queueRecord(agencyId, id, provider);
                    connection.commit();
                    success++;
                } catch (RawRepoException ex) {
                    failed++;
                    diags.add(new StandardResponse.Result.Diag("Record: " + id, ex.getMessage()));
                    Throwable cause = ex.getCause();
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
            StringBuilder message = new StringBuilder();
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

    void queueRecord(Integer agencyId, String bibliographicRecordId, String provider) throws RawRepoException {
        RawRepoDAO dao = getDao();

        String mimeType = MarcXChangeMimeType.MARCXCHANGE;
        if (dao.recordExistsMabyDeleted(bibliographicRecordId, agencyId)) {
            mimeType = dao.getMimeTypeOf(bibliographicRecordId, agencyId);
        }
        dao.changedRecord(provider, new RecordId(bibliographicRecordId, agencyId), mimeType);
    }
}
