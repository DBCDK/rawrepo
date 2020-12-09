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

import dk.dbc.marcx.MarcXParser;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RecordMetaDataHistory;
import dk.dbc.rawrepo.maintain.transport.StandardResponse;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.sql.DataSource;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class RevertRecords extends RawRepoWorker {

    private static final Logger log = LoggerFactory.getLogger(RevertRecords.class);

    RevertRecords(DataSource dataSource, VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector, ExecutorService executorService) {
        super(dataSource, vipCoreLibraryRulesConnector, executorService);
    }

    HashMap<String, ArrayList<String>> getValues(HashMap<String, List<String>> valuesSet, String leaving) {
        final HashMap<String, ArrayList<String>> values = new HashMap<>();

        try {
            values.put("provider", getProviders());
        } catch (SQLException ex) {
            log.error("Error getting values: " + ex.getMessage());
        }
        return values;
    }

    Object revertRecords(Integer agencyId, List<String> bibliographicRecordIds, long millis, String provider, String trackingId) {
        log.debug("agencyId = " + agencyId +
                "; bibliographicRecordIds = " + bibliographicRecordIds +
                "; time = " + new Date(millis) +
                "; provider = " + provider +
                "; trackingId = " + trackingId);
        final ArrayList<StandardResponse.Result.Diag> diags = new ArrayList<>();
        int success = 0;
        int failed = 0;
        try {
            final Connection connection = getConnection();
            for (String bibliographicRecordId : bibliographicRecordIds) {
                connection.setAutoCommit(false);
                try {
                    revertRecord(agencyId, bibliographicRecordId, millis, provider, trackingId);
                    connection.commit();
                    success++;
                } catch (RawRepoException ex) {
                    failed++;
                    diags.add(new StandardResponse.Result.Diag("Record: " + bibliographicRecordId, ex.getMessage()));
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
            final StringBuilder message = new StringBuilder();
            message.append("Done!");
            message.append("\n* Successfully reverted: ").append(success).append(" records.");
            if (failed != 0) {
                status = StandardResponse.Result.Status.PARTIAL;
                message.append("\n* Failed to revert: ").append(failed).append(" records.");
            }

            return new StandardResponse.Result(status, message.toString(), diags);
        } catch (SQLException ex) {
            log.error("Error getting database connection: " + ex.getMessage());
            return new StandardResponse.Result(StandardResponse.Result.Status.FAILURE, "Error getting database connection");
        }
    }

    void revertRecord(Integer agencyId, String bibliographicRecordId, long millis, String provider, String trackingId) throws RawRepoException {
        final RawRepoDAO dao = getDao();
        log.trace("millis = " + millis);
        if (!dao.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)) {
            throw new RawRepoException("Record does not exist");
        }

        final Record current = dao.fetchRecord(bibliographicRecordId, agencyId);
        final RecordId id = current.getId();
        long currentTime = current.getModified().toEpochMilli();
        log.trace("currentTime = " + currentTime);
        if (currentTime <= millis) {
            throw new RawRepoException("Record is already older");
        }
        final List<RecordMetaDataHistory> history = dao.getRecordHistory(bibliographicRecordId, agencyId);
        for (RecordMetaDataHistory oldRecord : history) {
            final long oldTime = oldRecord.getModified().toEpochMilli();
            log.trace("oldTime = " + oldTime);
            if (oldTime <= millis) {
                final Record historicRecord = dao.getHistoricRecord(oldRecord);
                if (historicRecord.isDeleted() && !current.isDeleted() && provider != null) {
                    dao.changedRecord(provider, id);
                }

                final Set<RecordId> relations = new HashSet<>();
                dao.setRelationsFrom(id, relations);
                historicRecord.setTrackingId(trackingId);
                historicRecord.setModified(Instant.now());
                dao.saveRecord(historicRecord);
                if (!historicRecord.isDeleted()) {
                    switch (historicRecord.getMimeType()) {
                        case MarcXChangeMimeType.MARCXCHANGE:
                            try {
                                final String parent = MarcXParser.getParent(new ByteArrayInputStream(historicRecord.getContent()));
                                if (parent != null) {
                                    int parentAgencyId = dao.findParentRelationAgency(parent, agencyId);
                                    relations.add(new RecordId(parent, parentAgencyId));
                                }

                                final List<RecordId> authorityLinks = MarcXParser.getAuthorityLinks(new ByteArrayInputStream(historicRecord.getContent()));
                                if (authorityLinks.size() > 0) {
                                    relations.addAll(authorityLinks);
                                }
                            } catch (ParserConfigurationException | SAXException | IOException ex) {
                                log.error(ex.getMessage());
                                throw new RawRepoException("Cannot parse record for parent relation");
                            }
                            break;
                        case MarcXChangeMimeType.ENRICHMENT:
                            int sibling = dao.findSiblingRelationAgency(bibliographicRecordId, agencyId);
                            relations.add(new RecordId(bibliographicRecordId, sibling));
                            break;
                        default:
                            break;
                    }
                    if (!relations.isEmpty()) {
                        dao.setRelationsFrom(id, relations);
                    }

                    if (provider != null) {
                        dao.changedRecord(provider, id);
                    }
                }
                return;
            }

        }
        throw new RawRepoException("Record doesn't have incarnation that old");
    }
}
