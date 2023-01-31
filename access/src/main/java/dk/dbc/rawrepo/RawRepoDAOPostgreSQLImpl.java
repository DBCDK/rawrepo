/*
 * dbc-rawrepo-access
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-access.
 *
 * dbc-rawrepo-access is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-access is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-access.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo;

import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class RawRepoDAOPostgreSQLImpl extends RawRepoDAO {

    private static final XLogger logger = XLoggerFactory.getXLogger(RawRepoDAOPostgreSQLImpl.class);

    private final Connection connection;
    private static final long TIMER_THRESHOLD_MS = 10;

    private static final int SCHEMA_VERSION = 28;
    private static final int SCHEMA_VERSION_COMPATIBLE = 26;

    private static final String VALIDATE_SCHEMA = "SELECT warning FROM version WHERE version=?";
    private static final String SELECT_RECORD = "SELECT deleted, mimetype, content, created, modified, trackingId FROM records WHERE bibliographicrecordid=? AND agencyid=?";
    private static final String INSERT_RECORD = "INSERT INTO records(bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified, trackingId) VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_RECORD = "UPDATE records SET deleted=?, mimetype=?, content=?, modified=?, trackingId=? WHERE bibliographicrecordid=? AND agencyid=?";
    private static final String SELECT_DELETED = "SELECT deleted FROM records WHERE bibliographicrecordid=? AND agencyid=?";
    private static final String SELECT_MIMETYPE = "SELECT mimetype FROM records WHERE bibliographicrecordid=? AND agencyid=?";

    private static final String HISTORIC_METADATA = "SELECT created, modified, deleted, mimetype, trackingId FROM records WHERE agencyid=? AND bibliographicrecordid=?" +
            " UNION SELECT created, modified, deleted, mimetype, trackingId FROM records_archive WHERE agencyid=? AND bibliographicrecordid=?" +
            " ORDER BY modified DESC";
    private static final String HISTORIC_CONTENT = "SELECT content FROM records WHERE agencyid=? AND bibliographicrecordid=? AND modified=?" +
            " UNION SELECT content FROM records_archive WHERE agencyid=? AND bibliographicrecordid=? AND modified=?";

    private static final String TRACKING_IDS_SINCE = "SELECT trackingid, modified FROM records" +
            " WHERE agencyid=? AND bibliographicrecordid = ? AND modified >= ?" +
            " UNION SELECT trackingid, modified FROM records_archive" +
            " WHERE agencyid=? AND bibliographicrecordid = ? AND modified >= ?" +
            " ORDER BY modified DESC;";

    private static final String SELECT_RELATIONS = "SELECT refer_bibliographicrecordid, refer_agencyid FROM relations WHERE bibliographicrecordid=? AND agencyid=?";
    private static final String SELECT_RELATIONS_PARENTS = "SELECT refer_bibliographicrecordid, refer_agencyid FROM relations WHERE bibliographicrecordid=? AND agencyid=? AND refer_bibliographicrecordid <> bibliographicrecordid";
    private static final String SELECT_RELATIONS_CHILDREN = "SELECT bibliographicrecordid, agencyid FROM relations WHERE refer_bibliographicrecordid=? AND refer_agencyid=? AND refer_bibliographicrecordid <> bibliographicrecordid";
    private static final String SELECT_RELATIONS_SIBLINGS_TO_ME = "SELECT bibliographicrecordid, agencyid FROM relations WHERE refer_bibliographicrecordid=? AND refer_agencyid=? AND refer_bibliographicrecordid = bibliographicrecordid";
    private static final String SELECT_RELATIONS_SIBLINGS_FROM_ME = "SELECT refer_bibliographicrecordid, refer_agencyid FROM relations WHERE bibliographicrecordid=? AND agencyid=? AND refer_bibliographicrecordid = bibliographicrecordid";
    private static final String SELECT_ALL_AGENCIES_FOR_ID = "SELECT agencyid FROM records WHERE bibliographicrecordid=?";
    private static final String SELECT_ALL_AGENCIES_FOR_ID_SKIP_DELETED = "SELECT agencyid FROM records WHERE bibliographicrecordid=? AND deleted='f'";
    private static final String DELETE_RELATIONS = "DELETE FROM relations WHERE bibliographicrecordid=? AND agencyid=?";
    private static final String INSERT_RELATION = "INSERT INTO relations (bibliographicrecordid, agencyid, refer_bibliographicrecordid, refer_agencyid) VALUES(?, ?, ?, ?)";

    private static final String CALL_ENQUEUE = "SELECT * FROM enqueue(?, ?, ?, ?, ?, ?)";
    private static final String CALL_ENQUEUE_BULK = "SELECT * FROM enqueue_bulk(?, ?, ?, ?, ?, ?)";
    private static final String CALL_DEQUEUE = "SELECT * FROM dequeue(?)";
    private static final String CALL_DEQUEUE_MULTI = "SELECT * FROM dequeue(?, ?)";
    private static final String QUEUE_ERROR = "INSERT INTO jobdiag(bibliographicrecordid, agencyid, worker, error, queued) VALUES(?, ?, ?, ?, ?)";
    private static final String CHECK_PROVIDER = "SELECT count(*) FROM queuerules WHERE provider = ?";

    private static final String TIME_ZONE = "SET TIME ZONE UTC";

    /**
     * Constructor
     *
     * @param connection Database connection
     */
    public RawRepoDAOPostgreSQLImpl(Connection connection) {
        this.connection = connection;
    }

    @Override
    protected void validateConnection() throws RawRepoException {
        try {
            try (PreparedStatement stmt = connection.prepareStatement(TIME_ZONE)) {
                stmt.executeUpdate();
            } catch (SQLException ex) {
                logger.error(TIME_ZONE + " error", ex);
                throw new RawRepoException("Unable to force timezone");
            }
            try (PreparedStatement stmt = connection.prepareStatement(VALIDATE_SCHEMA)) {
                stmt.setInt(1, SCHEMA_VERSION);
                try (ResultSet resultSet = stmt.executeQuery()) {
                    if (resultSet.next()) {
                        String warning = resultSet.getString(1);
                        if (warning != null) {
                            logger.warn(warning);
                        }
                        return;
                    }
                }
            }
            try (PreparedStatement stmt = connection.prepareStatement(VALIDATE_SCHEMA)) {
                stmt.setInt(1, SCHEMA_VERSION_COMPATIBLE);
                try (ResultSet resultSet = stmt.executeQuery()) {
                    if (resultSet.next()) {
                        String warning = resultSet.getString(1);
                        if (warning != null) {
                            logger.warn(warning);
                        }
                        return;
                    }
                }
            }
        } catch (SQLException ex) {
            logger.error("Validating schema", ex);
        }
        logger.error("Incompatible database schema software: {}", SCHEMA_VERSION);
        throw new RawRepoException("Incompatible database schema");
    }

    /**
     * Fetch a record from the database
     * <p>
     * Create one if none exists in the database
     *
     * @param bibliographicRecordId String with record bibliographicRecordId
     * @param agencyId              agencyId number
     * @return fetched / new Record
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public Record fetchRecord(String bibliographicRecordId, int agencyId) throws RawRepoException {
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_RECORD)) {
            stmt.setString(1, bibliographicRecordId);
            stmt.setInt(2, agencyId);

            try (ResultSet resultSet = stmt.executeQuery()) {
                if (resultSet.next()) {
                    final boolean deleted = resultSet.getBoolean("DELETED");
                    final String mimeType = resultSet.getString("MIMETYPE");
                    final String base64Content = resultSet.getString("CONTENT");
                    byte[] content = base64Content == null ? null : DatatypeConverter.parseBase64Binary(base64Content);
                    Instant created = resultSet.getTimestamp("CREATED").toInstant();
                    Instant modified = resultSet.getTimestamp("MODIFIED").toInstant();
                    String trackingId = resultSet.getString("TRACKINGID");
                    Record record = new RecordImpl(bibliographicRecordId, agencyId, deleted, mimeType, content, created, modified, trackingId, false);

                    resultSet.close();
                    stmt.close();
                    return record;
                }
            }
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching record", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.SELECT_RECORD(%s, %s)", bibliographicRecordId, agencyId));
        }
        return new RecordImpl(new RecordId(bibliographicRecordId, agencyId));
    }


    /**
     * Check for existence of a record
     *
     * @param bibliographicRecordId String with record bibliographicRecordId
     * @param agencyId              agencyId number
     * @return truth value for the existence of the record
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public boolean recordExists(String bibliographicRecordId, int agencyId) throws RawRepoException {
        Boolean recordDeleted = isRecordDeleted(bibliographicRecordId, agencyId);
        return recordDeleted != null && !recordDeleted;
    }

    /**
     * Check for existence of a record (possibly deleted)
     *
     * @param bibliographicRecordId String with record bibliographicRecordId
     * @param agencyId              agencyId number
     * @return truth value for the existence of the record
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public boolean recordExistsMaybeDeleted(String bibliographicRecordId, int agencyId) throws RawRepoException {
        Boolean recordDeleted = isRecordDeleted(bibliographicRecordId, agencyId);
        return recordDeleted != null;
    }

    @Override
    public List<RecordMetaDataHistory> getRecordHistory(String bibliographicRecordId, int agencyId) throws RawRepoException {
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try {
            ArrayList<RecordMetaDataHistory> ret = new ArrayList<>();
            try (PreparedStatement stmt = connection.prepareStatement(HISTORIC_METADATA)) {
                RecordId recordId = new RecordId(bibliographicRecordId, agencyId);
                int pos = 1;
                stmt.setInt(pos++, agencyId);
                stmt.setString(pos++, bibliographicRecordId);
                stmt.setInt(pos++, agencyId);
                stmt.setString(pos, bibliographicRecordId);
                try (ResultSet resultSet = stmt.executeQuery()) {
                    while (resultSet.next()) {
                        pos = 1;
                        Instant created = resultSet.getTimestamp(pos++).toInstant();
                        Instant modified = resultSet.getTimestamp(pos++).toInstant();
                        boolean deleted = resultSet.getBoolean(pos++);
                        String mimeType = resultSet.getString(pos++);
                        String trackingId = resultSet.getString(pos);
                        ret.add(new RecordMetaDataHistory(recordId, deleted, mimeType, created, modified, trackingId));
                    }
                }
            }
            for (RecordMetaDataHistory his : ret) {
                logger.info("RecordMetaDataHistory: {}", his);
            }

            return ret;
        } catch (SQLException ex) {
            throw new RawRepoException("Error getting record history", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.HISTORIC_METADATA(%s, %s)", bibliographicRecordId, agencyId));
        }
    }

    @Override
    public Record getHistoricRecord(RecordMetaDataHistory recordMetaData) throws RawRepoException {
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try {
            int agencyId = recordMetaData.getId().getAgencyId();
            String bibliographicRecordId = recordMetaData.getId().getBibliographicRecordId();
            Timestamp timestamp = Timestamp.from(recordMetaData.getTimestamp());
            try (PreparedStatement stmt = connection.prepareStatement(HISTORIC_CONTENT)) {
                int pos = 1;
                stmt.setInt(pos++, agencyId);
                stmt.setString(pos++, bibliographicRecordId);
                stmt.setTimestamp(pos++, timestamp);
                stmt.setInt(pos++, agencyId);
                stmt.setString(pos++, bibliographicRecordId);
                stmt.setTimestamp(pos, timestamp);
                try (ResultSet resultSet = stmt.executeQuery()) {
                    if (resultSet.next()) {
                        final String base64Content = resultSet.getString(1);
                        byte[] content = base64Content == null ? null : DatatypeConverter.parseBase64Binary(base64Content);
                        return new RecordImpl(bibliographicRecordId, agencyId, recordMetaData.isDeleted(),
                                recordMetaData.getMimeType(), content,
                                recordMetaData.getCreated(), recordMetaData.getModified(), recordMetaData.getTrackingId(), false);
                    }
                }
            }
            throw new RawRepoExceptionRecordNotFound("Error getting record history");
        } catch (SQLException | RawRepoExceptionRecordNotFound ex) {
            throw new RawRepoException("Error getting record history", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.HISTORIC_CONTENT(%s)", recordMetaData));
        }

    }

    @Override
    public List<String> getTrackingIdsSince(String bibliographicRecordId, int agencyId, Timestamp timestamp) throws RawRepoException {
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        ArrayList<String> list = new ArrayList<>();
        try {
            try (PreparedStatement stmt = connection.prepareStatement(TRACKING_IDS_SINCE)) {
                int pos = 1;
                stmt.setInt(pos++, agencyId);
                stmt.setString(pos++, bibliographicRecordId);
                stmt.setTimestamp(pos++, timestamp);
                stmt.setInt(pos++, agencyId);
                stmt.setString(pos++, bibliographicRecordId);
                stmt.setTimestamp(pos, timestamp);
                try (ResultSet resultSet = stmt.executeQuery()) {
                    while (resultSet.next()) {
                        list.add(resultSet.getString(1));
                    }
                    return list;
                }
            }
        } catch (SQLException ex) {
            throw new RawRepoException("Error getting record history", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.TRACKING_IDS_SINCE(%s, %s, %s)", bibliographicRecordId, agencyId, timestamp));
        }
    }

    private static final String LOG_DATABASE_ERROR = "Error accessing database";

    /**
     * Save a record to database after it has been modified
     * <p>
     * Will try to update, otherwise insert
     *
     * @param record record to be saved
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public void saveRecord(Record record) throws RawRepoException {
        if (record.getMimeType().isEmpty()) {
            throw new RawRepoException("Record has unset mimetype, cannot save");
        }
        StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (PreparedStatement stmt = connection.prepareStatement(UPDATE_RECORD)) {
            int pos = 1;
            stmt.setBoolean(pos++, record.isDeleted());
            stmt.setString(pos++, record.getMimeType());
            stmt.setString(pos++, DatatypeConverter.printBase64Binary(record.getContent()));
            stmt.setTimestamp(pos++, Timestamp.from(record.getModified()));
            stmt.setString(pos++, record.getTrackingId());
            stmt.setString(pos++, record.getId().getBibliographicRecordId());
            stmt.setInt(pos, record.getId().getAgencyId());
            if (stmt.executeUpdate() > 0) {
                stmt.close();
                return;
            }
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error updating record", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.UPDATE_RECORD(%s:%s)",
                    record.getId().getBibliographicRecordId(), record.getId().getAgencyId()));
        }
        try (PreparedStatement stmt = connection.prepareStatement(INSERT_RECORD)) {
            int pos = 1;
            stmt.setString(pos++, record.getId().getBibliographicRecordId());
            stmt.setInt(pos++, record.getId().getAgencyId());
            stmt.setBoolean(pos++, record.isDeleted());
            stmt.setString(pos++, record.getMimeType());
            stmt.setString(pos++, DatatypeConverter.printBase64Binary(record.getContent()));
            stmt.setTimestamp(pos++, Timestamp.from(record.getCreated()));
            stmt.setTimestamp(pos++, Timestamp.from(record.getModified()));
            stmt.setString(pos, record.getTrackingId());
            stmt.execute();
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error saving record", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.INSERT_RECORD(%s:%s)",
                    record.getId().getBibliographicRecordId(), record.getId().getAgencyId()));
        }
        if (record instanceof RecordImpl) {
            ((RecordImpl) record).original = false;
        }
    }

    @Override
    public String getMimeTypeOf(String bibliographicRecordId, int agencyId) throws RawRepoException {
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_MIMETYPE)) {
            int pos = 1;
            stmt.setString(pos++, bibliographicRecordId);
            stmt.setInt(pos, agencyId);
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString(1);
                }
            }
            throw new RawRepoExceptionRecordNotFound("Failed to find mimetype for recordId " + bibliographicRecordId + ", agencyId " + agencyId);
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching mimetype", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.SELECT_MIMETYPE(%s, %s)", bibliographicRecordId, agencyId));
        }
    }

    @Override
    protected Map<RecordId, String> getMimeTypeOfList(Set<RecordId> originalRecordIds) throws RawRepoException {
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        final Map<RecordId, String> result = new HashMap<>();

        // If input is empty then return empty result
        if (originalRecordIds.isEmpty()) {
            return result;
        }

        final int sliceSize = 1000;
        int index = 0;
        while (index < originalRecordIds.size()) {
            final Set<RecordId> recordIds = originalRecordIds.stream()
                    .skip(index)
                    .limit(sliceSize)
                    .collect(Collectors.toSet());
            index += sliceSize;

            final StringBuilder query = new StringBuilder();
            query.append("SELECT bibliographicrecordid, agencyid, mimetype FROM records WHERE (bibliographicrecordid, agencyid) IN (");
            final List<String> placeHolders = new ArrayList<>();
            for (int i = 0; i < recordIds.size(); i++) {
                placeHolders.add("(?, ?)");
            }
            query.append(String.join(",", placeHolders));
            query.append(")");

            try (PreparedStatement stmt = connection.prepareStatement(query.toString())) {
                int pos = 1;
                for (RecordId recordId : recordIds) {
                    stmt.setString(pos++, recordId.getBibliographicRecordId());
                    stmt.setInt(pos++, recordId.getAgencyId());
                }
                try (ResultSet resultSet = stmt.executeQuery()) {
                    while (resultSet.next()) {
                        final String bibliographicRecordId = resultSet.getString(1);
                        final int agencyId = resultSet.getInt(2);
                        final String mimetype = resultSet.getString(3);

                        result.put(new RecordId(bibliographicRecordId, agencyId), mimetype);
                    }
                }


            } catch (SQLException ex) {
                logger.error(LOG_DATABASE_ERROR, ex);
                throw new RawRepoException("Error fetching mimetype", ex);
            } finally {
                watch.stop(String.format("rawrepo.query.SELECT_MIMETYPE_LIST for %s RecordIds", originalRecordIds.size()));
            }
        }
        return result;
    }

    @Override
    protected Boolean isRecordDeleted(String bibliographicRecordId, int agencyId) throws RawRepoException {
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_DELETED)) {
            int pos = 1;
            stmt.setString(pos++, bibliographicRecordId);
            stmt.setInt(pos, agencyId);
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getBoolean(1);
                }
            }
            return null;
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching deleted state", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.SELECT_DELETED(%s, %s)", bibliographicRecordId, agencyId));
        }
    }

    @Override
    protected HashMap<RecordId, Boolean> isRecordDeletedList(Set<RecordId> originalRecordIds) throws RawRepoException {
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        final HashMap<RecordId, Boolean> result = new HashMap<>();

        if (originalRecordIds.isEmpty()) {
            return result;
        }

        final int sliceSize = 1000;
        int index = 0;
        while (index < originalRecordIds.size()) {
            final Set<RecordId> recordIds = originalRecordIds.stream()
                    .skip(index)
                    .limit(sliceSize)
                    .collect(Collectors.toSet());
            index += sliceSize;

            final StringBuilder query = new StringBuilder();
            query.append("SELECT bibliographicrecordid, agencyid, deleted FROM records where (bibliographicrecordid, agencyid) in (");
            final List<String> placeHolders = new ArrayList<>();
            for (int i = 0; i < recordIds.size(); i++) {
                placeHolders.add("(?, ?)");
            }
            query.append(String.join(",", placeHolders));
            query.append(")");

            try (PreparedStatement stmt = connection.prepareStatement(query.toString())) {
                int pos = 1;
                for (RecordId recordId : recordIds) {
                    stmt.setString(pos++, recordId.getBibliographicRecordId());
                    stmt.setInt(pos++, recordId.getAgencyId());
                }
                try (ResultSet resultSet = stmt.executeQuery()) {
                    while (resultSet.next()) {
                        final String bibliographicRecordId = resultSet.getString(1);
                        final int agencyId = resultSet.getInt(2);
                        final boolean deleted = resultSet.getBoolean(3);

                        final RecordId recordId = new RecordId(bibliographicRecordId, agencyId);
                        result.put(recordId, deleted);
                    }
                }
            } catch (SQLException ex) {
                logger.error(LOG_DATABASE_ERROR, ex);
                throw new RawRepoException("Error fetching deleted state", ex);
            } finally {
                watch.stop(String.format("rawrepo.query.SELECT_DELETED_BULK for %s RecordIds", originalRecordIds.size()));
            }
        }
        return result;
    }


    /**
     * Get a collection of my "dependencies". All relations that
     * bibliographicRecordId have
     *
     * @param recordId complex key for a record
     * @return collection of record ids of whom bibliographicRecordId depends
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public Set<RecordId> getRelationsFrom(RecordId recordId) throws RawRepoException {
        Set<RecordId> collection = new HashSet<>();
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_RELATIONS)) {
            int pos = 1;
            stmt.setString(pos++, recordId.getBibliographicRecordId());
            stmt.setInt(pos, recordId.getAgencyId());
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    collection.add(new RecordId(resultSet.getString(1), resultSet.getInt(2)));
                }
            }
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching getRelationsFrom relations", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.SELECT_RELATIONS(%s)", recordId.toString()));
        }
        return collection;
    }

    /**
     * Delete all relations related from an bibliographicRecordId
     *
     * @param recordId complex key of bibliographicRecordId
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public void deleteRelationsFrom(RecordId recordId) throws RawRepoException {
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (PreparedStatement stmt = connection.prepareStatement(DELETE_RELATIONS)) {
            int pos = 1;
            stmt.setString(pos++, recordId.getBibliographicRecordId());
            stmt.setInt(pos, recordId.getAgencyId());
            stmt.execute();
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error deleting relations", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.DELETE_RELATIONS(%s)", recordId));
        }
    }

    /**
     * Clear all existing relations and set the new ones
     *
     * @param recordId recordid to update
     * @param refers   collection of record ids bibliographicRecordId depends on
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public void setRelationsFrom(RecordId recordId, Set<RecordId> refers) throws RawRepoException {
        ValidateRelations.validate(this, recordId, refers);

        deleteRelationsFrom(recordId);
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (PreparedStatement stmt = connection.prepareStatement(INSERT_RELATION)) {
            int pos = 1;
            stmt.setString(pos++, recordId.getBibliographicRecordId());
            stmt.setInt(pos++, recordId.getAgencyId());
            for (RecordId refer : refers) {
                int p = pos;
                stmt.setString(p++, refer.getBibliographicRecordId());
                stmt.setInt(p, refer.getAgencyId());
                stmt.execute();
            }
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error setting relations", ex);
        } finally {
            watch.stop("rawrepo.query.INSERT_RELATION");
        }
    }

    /**
     * Get all records who has bibliographicRecordId as relation, but no
     * siblings
     *
     * @param recordId recordid to find relations to
     * @return collection of record ids that list bibliographicRecordId at
     * relation
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public Set<RecordId> getRelationsChildren(RecordId recordId) throws RawRepoException {
        Set<RecordId> collection = new HashSet<>();
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_RELATIONS_CHILDREN)) {
            int pos = 1;
            stmt.setString(pos++, recordId.getBibliographicRecordId());
            stmt.setInt(pos, recordId.getAgencyId());
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    collection.add(new RecordId(resultSet.getString(1), resultSet.getInt(2)));
                }
            }
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching getRelationsChildren relations", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.SELECT_RELATIONS_CHILDREN(%s)", recordId.toString()));
        }
        return collection;
    }

    /**
     * Get a collection of my "dependencies". All relations that
     * bibliographicRecordId have
     *
     * @param recordId complex key for a record
     * @return collection of record ids of whom bibliographicRecordId depends
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public Set<RecordId> getRelationsParents(RecordId recordId) throws RawRepoException {
        Set<RecordId> collection = new HashSet<>();
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_RELATIONS_PARENTS)) {
            int pos = 1;
            stmt.setString(pos++, recordId.getBibliographicRecordId());
            stmt.setInt(pos, recordId.getAgencyId());
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    collection.add(new RecordId(resultSet.getString(1), resultSet.getInt(2)));
                }
            }
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching getRelationsParents relations", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.SELECT_RELATIONS_PARENTS(%s)", recordId.toString()));
        }
        return collection;
    }

    @Override
    protected Set<RelationsPair> getAllChildRelations(RecordId recordId) throws RawRepoException {
        final Set<RelationsPair> result = getAllChildRelations(new HashSet<>(Collections.singletonList(recordId)));

        Set<RecordId> children = result.stream().map(RelationsPair::getChild).collect(Collectors.toSet());
        while (!children.isEmpty()) {
            Set<RelationsPair> tmp = getAllChildRelations(children);
            result.addAll(tmp);
            children = tmp.stream().map(RelationsPair::getChild).collect(Collectors.toSet());
        }

        return result;
    }

    private Set<RelationsPair> getAllChildRelations(Set<RecordId> originalRecordIds) throws RawRepoException {
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        final Set<RelationsPair> result = new HashSet<>();

        // If input is empty then return empty result
        if (originalRecordIds.isEmpty()) {
            return result;
        }

        final int sliceSize = 1000;
        int index = 0;
        while (index < originalRecordIds.size()) {
            final Set<RecordId> recordIds = originalRecordIds.stream()
                    .skip(index)
                    .limit(sliceSize)
                    .collect(Collectors.toSet());
            index += sliceSize;
            final StringBuilder query = new StringBuilder();
            query.append("SELECT bibliographicrecordid, agencyid, refer_bibliographicrecordid, refer_agencyid FROM relations WHERE (refer_bibliographicrecordid, refer_agencyid) IN (");
            final List<String> placeHolders = new ArrayList<>();
            for (int i = 0; i < recordIds.size(); i++) {
                placeHolders.add("(?, ?)");
            }
            query.append(String.join(",", placeHolders));
            query.append(")");

            try (PreparedStatement stmt = connection.prepareStatement(query.toString())) {
                int pos = 1;
                for (RecordId recordId : recordIds) {
                    stmt.setString(pos++, recordId.getBibliographicRecordId());
                    stmt.setInt(pos++, recordId.getAgencyId());
                }
                try (ResultSet resultSet = stmt.executeQuery()) {
                    while (resultSet.next()) {
                        final String bibliographicRecordId = resultSet.getString(1);
                        final int agencyId = resultSet.getInt(2);
                        final String referBibliographicRecordId = resultSet.getString(3);
                        final int referAgencyId = resultSet.getInt(4);

                        final RecordId recordId = new RecordId(bibliographicRecordId, agencyId);
                        final RecordId referRecordId = new RecordId(referBibliographicRecordId, referAgencyId);

                        result.add(new RelationsPair(recordId, referRecordId));
                    }
                }
            } catch (SQLException ex) {
                logger.error(LOG_DATABASE_ERROR, ex);
                throw new RawRepoException("Error fetching mimetype", ex);
            } finally {
                watch.stop(String.format("rawrepo.query.SELECT_RELATIONS_CHILDREN_LIST for %s RecordIds",
                        originalRecordIds.size()));
            }
        }
        return result;
    }


    /**
     * @param recordId complex key for a record
     * @return siblings collection
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public Set<RecordId> getRelationsSiblingsToMe(RecordId recordId) throws RawRepoException {
        Set<RecordId> collection = new HashSet<>();
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_RELATIONS_SIBLINGS_TO_ME)) {
            int pos = 1;
            stmt.setString(pos++, recordId.getBibliographicRecordId());
            stmt.setInt(pos, recordId.getAgencyId());
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    collection.add(new RecordId(resultSet.getString(1), resultSet.getInt(2)));
                }
            }
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching getRelationsSiblingsToMe relations", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.SELECT_RELATIONS_SIBLINGS_TO_ME(%s)", recordId.toString()));
        }
        return collection;
    }

    /**
     * @param recordId complex key for a record
     * @return set of relation siblings
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public Set<RecordId> getRelationsSiblingsFromMe(RecordId recordId) throws RawRepoException {
        Set<RecordId> collection = new HashSet<>();
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_RELATIONS_SIBLINGS_FROM_ME)) {
            int pos = 1;
            stmt.setString(pos++, recordId.getBibliographicRecordId());
            stmt.setInt(pos, recordId.getAgencyId());
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    collection.add(new RecordId(resultSet.getString(1), resultSet.getInt(2)));
                }
            }
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching getRelationsSiblingsFromMe relations", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.SELECT_RELATIONS_SIBLINGS_FROM_ME(%s)", recordId.toString()));
        }
        return collection;
    }

    /**
     * Get all libraries that has local data to bibliographicRecordId
     *
     * @param bibliographicRecordId record bibliographicRecordId
     * @return Collection of libraries that has localdata for this record
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public Set<Integer> allAgenciesForBibliographicRecordId(String bibliographicRecordId) throws RawRepoException {
        Set<Integer> collection = new HashSet<>();
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_ALL_AGENCIES_FOR_ID)) {
            stmt.setString(1, bibliographicRecordId);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    collection.add(resultSet.getInt(1));
                }
            }
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching allAgenciesForBibliographicRecordId relations", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.SELECT_ALL_AGENCIES_FOR_ID(%s)", bibliographicRecordId));
        }
        return collection;
    }

    /**
     * Get all libraries that has local data to bibliographicRecordId that is not Marked deleted
     *
     * @param bibliographicRecordId record bibliographicRecordId
     * @return Collection of libraries that has localData for this record
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public Set<Integer> allAgenciesForBibliographicRecordIdSkipDeleted(String bibliographicRecordId) throws RawRepoException {
        Set<Integer> collection = new HashSet<>();
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_ALL_AGENCIES_FOR_ID_SKIP_DELETED)) {
            stmt.setString(1, bibliographicRecordId);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    collection.add(resultSet.getInt(1));
                }
            }
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching allAgenciesForBibliographicRecordIdSkipDeleted relations", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.SELECT_ALL_AGENCIES_FOR_ID_SKIP_DELETED(%s)", bibliographicRecordId));
        }
        return collection;
    }


    /**
     * Put job(s) on the queue (in the database)
     *
     * @param job      job description
     * @param provider change initiator
     * @param changed  is job for a record that has been changed
     * @param leaf     is this job for a tree leaf
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public void enqueue(RecordId job, String provider, boolean changed, boolean leaf) throws RawRepoException {
        enqueue(job, provider, changed, leaf, 1000);
    }

    @Override
    public void enqueue(RecordId job, String provider, boolean changed, boolean leaf, int priority) throws RawRepoException {
        StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (PreparedStatement stmt = connection.prepareStatement(CALL_ENQUEUE)) {
            int pos = 1;
            stmt.setString(pos++, job.getBibliographicRecordId());
            stmt.setInt(pos++, job.getAgencyId());
            stmt.setString(pos++, provider);
            stmt.setString(pos++, changed ? "Y" : "N");
            stmt.setString(pos++, leaf ? "Y" : "N");
            stmt.setInt(pos, priority);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    if (resultSet.getBoolean(2)) {
                        logger.info("Queued: worker = {}; job = {}", resultSet.getString(1), job);
                    } else {
                        logger.info("Queued: worker = {}; job = {}; skipped - already on queue", resultSet.getString(1), job);
                    }
                }
            }
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error queueing job", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.CALL_ENQUEUE(%s:%s, %s, %s, %s, %s)",
                    job.getBibliographicRecordId(), job.getAgencyId(), provider, changed, leaf, priority));
        }
    }

    @Override
    protected void enqueueBulk(List<EnqueueJob> jobs) throws RawRepoException {
        List<String> bibliographicRecordIdList = new ArrayList<>();
        List<Integer> agencyList = new ArrayList<>();
        List<String> providerList = new ArrayList<>();
        List<String> changedList = new ArrayList<>();
        List<String> leafList = new ArrayList<>();
        List<Integer> priorityList = new ArrayList<>();

        StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        for (EnqueueJob job : jobs) {
            bibliographicRecordIdList.add(job.getJob().getBibliographicRecordId());
            agencyList.add(job.getJob().getAgencyId());
            providerList.add(job.getProvider());
            changedList.add(job.isChanged() ? "Y" : "N");
            leafList.add(job.isLeaf() ? "Y" : "N");
            priorityList.add(job.getPriority());
        }

        try (CallableStatement stmt = connection.prepareCall(CALL_ENQUEUE_BULK)) {
            stmt.setArray(1, stmt.getConnection().createArrayOf("varchar", bibliographicRecordIdList.toArray()));
            stmt.setArray(2, stmt.getConnection().createArrayOf("int4", agencyList.toArray()));
            stmt.setArray(3, stmt.getConnection().createArrayOf("varchar", providerList.toArray()));
            stmt.setArray(4, stmt.getConnection().createArrayOf("varchar", changedList.toArray()));
            stmt.setArray(5, stmt.getConnection().createArrayOf("varchar", leafList.toArray()));
            stmt.setArray(6, stmt.getConnection().createArrayOf("int4", priorityList.toArray()));

            stmt.executeQuery();
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error during enqueue_bulk", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.CALL_ENQUEUE_BULK for %s EnqueueJobs", jobs.size()));
        }

    }

    public boolean checkProvider(String provider) throws RawRepoException {
        logger.info("Check provider: {}", provider);

        int count = 0;
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (CallableStatement stmt = connection.prepareCall(CHECK_PROVIDER)) {
            stmt.setString(1, provider);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    count = resultSet.getInt(1);
                }
            }

            return count > 0;
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error checking provider", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.CHECK_PROVIDER(%s)", provider));
        }
    }

    /**
     * Pull a job from the queue
     *
     * @param worker name of worker that want's to take a job
     * @return job description
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public List<QueueJob> dequeue(String worker, int wanted) throws RawRepoException {
        List<QueueJob> result = new ArrayList<>();
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (CallableStatement stmt = connection.prepareCall(CALL_DEQUEUE_MULTI)) {
            int pos = 1;
            stmt.setString(pos++, worker);
            stmt.setInt(pos, wanted);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    QueueJob job = new QueueJob(resultSet.getString("bibliographicrecordid"),
                            resultSet.getInt("agencyid"),
                            resultSet.getString("worker"),
                            resultSet.getTimestamp("queued"),
                            resultSet.getInt("priority"));
                    result.add(job);
                    logger.debug("Dequeued job = {}; worker = {}", job, worker);
                }
                return result;
            }
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error dequeueing jobs", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.CALL_DEQUEUE_MULTI(%s, %s)", worker, wanted));
        }
    }

    /**
     * Pull a job from the queue
     *
     * @param worker name of worker that want's to take a job
     * @return job description
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public QueueJob dequeue(String worker) throws RawRepoException {
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (CallableStatement stmt = connection.prepareCall(CALL_DEQUEUE)) {
            stmt.setString(1, worker);
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (resultSet.next()) {
                    QueueJob job = new QueueJob(resultSet.getString("bibliographicrecordid"),
                            resultSet.getInt("agencyid"),
                            resultSet.getString("worker"),
                            resultSet.getTimestamp("queued"),
                            resultSet.getInt("priority"));
                    logger.debug("Dequeued job = {}; worker = {}", job, worker);
                    return job;
                }
                return null;
            }
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error dequeueing job", ex);
        } finally {
            watch.stop(String.format("rawrepo.query.CALL_DEQUEUE(%s)", worker));
        }
    }

    /**
     * QueueJob has failed, log to database
     *
     * @param queueJob job that failed
     * @param error    what happened (empty string not allowed)
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public void queueFail(QueueJob queueJob, String error) throws RawRepoException {
        if (error == null || error.equals("")) {
            throw new RawRepoException("Error cannot be empty in queueFail");
        }
        final StopWatch watch = new Log4JStopWatch().setTimeThreshold(TIMER_THRESHOLD_MS);
        try (PreparedStatement stmt = connection.prepareStatement(QUEUE_ERROR)) {
            int pos = 1;
            stmt.setString(pos++, queueJob.job.bibliographicRecordId);
            stmt.setInt(pos++, queueJob.job.agencyId);
            stmt.setString(pos++, queueJob.worker);
            stmt.setString(pos++, error);
            stmt.setTimestamp(pos, queueJob.queued);
            stmt.executeUpdate();
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error reporting job status", ex);
        } finally {
            watch.stop("rawrepo.query.QUEUE_ERROR");
        }
    }

    /**
     * QueueJob has failed
     *
     * @param queueJob job that failed
     * @param error    what happened (empty string not allowed)
     * @throws RawRepoException when something goes wrong
     */
    @Override
    public void queueFailWithSavepoint(QueueJob queueJob, String error) throws RawRepoException {
        if (error == null || error.equals("")) {
            throw new RawRepoException("Error cannot be empty in queueFail");
        }
        try (PreparedStatement rollback = connection.prepareStatement("ROLLBACK TO DEQUEUED")) {
            rollback.execute();
        } catch (SQLException ex) {
            logger.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error rolling back", ex);
        }
        queueFail(queueJob, error);
    }

}
