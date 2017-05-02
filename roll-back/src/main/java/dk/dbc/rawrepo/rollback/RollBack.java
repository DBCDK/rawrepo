/*
 * dbc-rawrepo-rollback
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-rollback.
 *
 * dbc-rawrepo-rollback is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-rollback is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-rollback.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.rollback;

import dk.dbc.rawrepo.QueueTarget;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RecordMetaDataHistory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RollBack {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(RollBack.class);

    private final static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public enum State {

        Keep("Keep the current state of the record"),
        Rollback("Roll back the state of the record"),
        Delete("Set the record to deleted"),
        Active("Set the record to not deleted");

        private final String description;

        State(String description) {
            this.description = description;
        }

        public String getDescription() {
            return name() + " - " + description;
        }

    }

    private static RecordMetaDataHistory findMatching(Date matchDate, DateMatch.Match matchType,
                                                      List<RecordMetaDataHistory> recordHistory) {
        RecordMetaDataHistory matching;
        switch (matchType) {
            case Equal: {
                matching = DateMatch.equal(matchDate, recordHistory);
                break;
            }
            case Before: {
                matching = DateMatch.before(matchDate, recordHistory);
                break;
            }
            case BeforeOrEqual: {
                matching = DateMatch.beforeOrSame(matchDate, recordHistory);
                break;
            }
            case After: {
                matching = DateMatch.after(matchDate, recordHistory);
                break;
            }
            case AfterOrEqual: {
                matching = DateMatch.afterOrSame(matchDate, recordHistory);
                break;
            }
            default: {
                log.error("Unsupported match type '{}'", matchType);
                matching = null;
                break;
            }
        }
        return matching;
    }

    static boolean getNewDeleted(State state, Record currentRecord, Record historicRecord) {
        boolean deleted;
        switch (state) {
            case Keep: {
                deleted = currentRecord.isDeleted();
                break;
            }
            case Rollback: {
                deleted = historicRecord.isDeleted();
                break;
            }
            case Delete: {
                deleted = true;
                break;
            }
            case Active: {
                deleted = false;
                break;
            }
            default: {
                log.error("Unsupported state option '{}'", state);
                deleted = historicRecord.isDeleted();
                break;
            }
        }
        return deleted;
    }

    public static boolean rollbackRecord(Connection connection, RecordId id, Date matchDate, DateMatch.Match matchType, State state, String queueRole, QueueTarget queueTarget) throws RawRepoException {
        log.info("Rolling record {} back to {}", id, matchDate);
        RawRepoDAO dao = RawRepoDAO.builder(connection).queue(queueTarget).build();
        boolean modified = rollbackRecord(dao, id, matchDate, matchType, state);
        if (modified) {
            dao.changedRecord(queueRole, id);
        }
        return modified;
    }

    static boolean rollbackRecord(RawRepoDAO dao, RecordId id, Date matchDate, DateMatch.Match matchType, State state) throws RawRepoException {

        List<RecordMetaDataHistory> recordHistory = dao.getRecordHistory(id.getBibliographicRecordId(), id.getAgencyId());

        RecordMetaDataHistory matching = findMatching(matchDate, matchType, recordHistory);
        if (matching != null) {
            log.debug("Rolling record {} back to {}", id, dateFormat.format(matchDate));

            Record currentRecord = dao.fetchRecord(id.getBibliographicRecordId(), id.getAgencyId());
            Record historicRecord = dao.getHistoricRecord(matching);

            log.debug("Comparing found record date {} to current record date {}",
                      dateFormat.format(matching.getModified()), dateFormat.format(currentRecord.getModified()));
            if (!currentRecord.getModified().equals(matching.getModified())) {

                boolean deleted = getNewDeleted(state, currentRecord, historicRecord);

                if (historicRecord.isDeleted() != deleted || currentRecord.isDeleted() != deleted) {
                    log.debug("Current deleted state {}, old deleted state {}, new deleted state {}",
                              currentRecord.isDeleted(), historicRecord.isDeleted(), deleted);
                }
                if (currentRecord.isDeleted() && !deleted) {
                    log.warn("Undeleting record {}. Relations are not restored", id);
                }
                historicRecord.setDeleted(deleted);
                historicRecord.setModified(new Date());
                historicRecord.setTrackingId("Rollback:" + matchType.getOperator() + matchDate);
                dao.saveRecord(historicRecord);
                return true;
            } else {
                log.debug("Record {} is already at required date {}", id, dateFormat.format(currentRecord.getModified()));
                return false;
            }
        } else {
            log.debug("No matching history for record {} to {} in history: {}", id, dateFormat.format(matchDate), recordHistory);
            return false;
        }
    }

    static Set<String> getRecordIds(Connection connection, int agencyId) throws SQLException {
        Set<String> set = new HashSet<>();
        try (PreparedStatement stmt = connection.prepareStatement("SELECT bibliographicrecordid FROM records WHERE agencyid = ?")) {
            stmt.setInt(1, agencyId);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    String recordId = resultSet.getString(1);
                    log.trace("Adding agency {} record '{}' ", agencyId, recordId);
                    set.add(recordId);
                }
            }
        }
        return set;
    }

    static void rollbackRecords( int agencyId, Iterable<String> ids, RawRepoDAO dao, Date matchDate, DateMatch.Match matchType, State state, String queueRole ) {
        int success = 0;
        int skipped = 0;
        int failed = 0;
        HashSet<String> idSet = new HashSet<>();

        for (String id : ids) {
            RecordId recordId = new RecordId(id, agencyId);
            try {
                if (rollbackRecord(dao, recordId, matchDate, matchType, state)) {
                    idSet.add(id);
                    success++;
                } else {
                    skipped++;
                }
            } catch (RawRepoException ex) {
                failed++;
                log.error("Failed to rollback record " + recordId, ex);
            }
            if (( success + skipped + failed ) % 1000 == 0) {
                log.info("Rolled back {}, skipped {}, failed {}", success, skipped, failed);
            }
            if (queueRole != null) {
                try {
                    queueRecords(dao, agencyId, idSet, queueRole);
                } catch (RawRepoException ex) {
                    log.error("Error queueing record: " + ex.getMessage());
                    log.debug("Error queueing record:", ex);
                }
            }
        }
        log.info("Rolled back {}, skipped {}, failed {}. Done.", success, skipped, failed);
    }

    static void queueRecords( RawRepoDAO dao, int agencyId, Set<String> ids, String role ) throws RawRepoException {
        int no = 0;
        log.info( "Queing {} ids", ids.size() );

        for ( String id : ids ) {
            dao.changedRecord(role, new RecordId( id, agencyId ));
            if ( ++no % 1000 == 0 ) {
                log.info( "Queued {}", no );
            }
        }
        log.info( "Queued {}", no );
    }

    public static void rollbackAgency( Connection connection, RawRepoDAO dao, int agencyId, Date matchDate, DateMatch.Match matchType, State state, String queueRole ) throws RawRepoException {

        log.info("Identifying records for {}", agencyId);
        try {
            Set<String> ids = getRecordIds(connection, agencyId);
            log.info("Rolling back {} records for agency {}. Matching {} as '{}'",
                     ids.size(), agencyId, matchDate, matchType);

            rollbackRecords(agencyId, ids, dao, matchDate, matchType, state, queueRole);
            if (queueRole != null) {
                queueRecords(dao, agencyId, ids, queueRole);
            }
        } catch (SQLException ex) {
            throw new RawRepoException(ex);
        }
    }

}
