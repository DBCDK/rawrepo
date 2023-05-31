package dk.dbc.rawrepo.rollback;

import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RecordMetaDataHistory;
import dk.dbc.vipcore.exception.VipCoreException;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 *
 */
class RollBack {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(RollBack.class);

    private final static DateTimeFormatter dateFormat =
            DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT)
                    .withLocale(Locale.UK)
                    .withZone(ZoneId.systemDefault());

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

    private static RecordMetaDataHistory findMatching(Instant matchDate, DateMatch.Match matchType,
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

    public static boolean rollbackRecord(Connection connection, RecordId id, Instant matchDate, DateMatch.Match matchType, State state, String queueRole) throws RawRepoException, VipCoreException {
        log.info("Rolling record {} back to {}", id, matchDate);
        RawRepoDAO dao = RawRepoDAO.builder(connection).build();
        boolean modified = rollbackRecord(dao, id, matchDate, matchType, state);
        if (modified) {
            queueRecord(dao, id, queueRole);
        }
        return modified;
    }

    private static boolean rollbackRecord(RawRepoDAO dao, RecordId id, Instant matchDate, DateMatch.Match matchType, State state) throws RawRepoException {
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
                historicRecord.setModified(Instant.now());
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

    private static void queueRecord(RawRepoDAO dao, RecordId id, String role) throws RawRepoException, VipCoreException {
        dao.changedRecord(role, id);
    }

    private static Set<String> getRecordIds(Connection connection, int agencyId) throws SQLException {
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

    static void rollbackRecords(int agencyId, Iterable<String> ids, RawRepoDAO dao, Instant matchDate, DateMatch.Match matchType, State state) {
        int success = 0;
        int skipped = 0;
        int failed = 0;

        for (String id : ids) {
            RecordId recordId = new RecordId(id, agencyId);
            try {
                if (rollbackRecord(dao, recordId, matchDate, matchType, state)) {
                    success++;
                } else {
                    skipped++;
                }
            } catch (RawRepoException ex) {
                failed++;
                log.error("Failed to rollback record " + recordId, ex);
            }
            if ((success + skipped + failed) % 1000 == 0) {
                log.info("Rolled back {}, skipped {}, failed {}", success, skipped, failed);
            }
        }
        log.info("Rolled back {}, skipped {}, failed {}. Done.", success, skipped, failed);
    }

    private static void queueRecords(RawRepoDAO dao, int agencyId, Set<String> ids, String role) throws RawRepoException, VipCoreException {
        int no = 0;
        log.info("Queueing {} ids", ids.size());

        for (String id : ids) {
            queueRecord(dao, new RecordId(id, agencyId), role);
            if (++no % 1000 == 0) {
                log.info("Queued {}", no);
            }
        }
        log.info("Queued {}", no);
    }

    static void rollbackAgency(Connection connection, int agencyId, Instant matchDate, DateMatch.Match matchType, State state, String queueRole) throws RawRepoException {
        RawRepoDAO dao = RawRepoDAO.builder(connection).build();

        log.info("Identifying records for {}", agencyId);
        try {
            Set<String> ids = getRecordIds(connection, agencyId);
            log.info("Rolling back {} records for agency {}. Matching {} as '{}'",
                    ids.size(), agencyId, matchDate, matchType);

            rollbackRecords(agencyId, ids, dao, matchDate, matchType, state);
            if (queueRole != null) {
                queueRecords(dao, agencyId, ids, queueRole);
            }
        } catch (SQLException | VipCoreException ex) {
            throw new RawRepoException(ex);
        }
    }

}
