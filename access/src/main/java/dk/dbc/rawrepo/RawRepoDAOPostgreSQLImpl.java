/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-commons
 *
 * dbc-rawrepo-commons is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-commons is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.xml.bind.DatatypeConverter;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class RawRepoDAOPostgreSQLImpl extends RawRepoDAO {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(RawRepoDAOPostgreSQLImpl.class);

    private final Connection connection;

    private static final int SCHEMA_VERSION = 8;

    private static final String VALIDATE_SCHEMA = "SELECT warning FROM version WHERE version=?";
    private static final String SELECT_RECORD = "SELECT deleted, mimetype, content, created, modified FROM records WHERE bibliographicrecordid=? AND agencyid=?";
    private static final String PURGE_RECORD = "DELETE FROM records WHERE bibliographicrecordid=? AND agencyid=?";
    private static final String INSERT_RECORD = "INSERT INTO records(bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified) VALUES(?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_RECORD = "UPDATE records SET deleted=?, mimetype=?, content=?, modified=? WHERE bibliographicrecordid=? AND agencyid=?";
    private static final String SELECT_DELETED = "SELECT deleted FROM records WHERE bibliographicrecordid=? AND agencyid=?";
    private static final String SELECT_MIMETYPE = "SELECT mimetype FROM records WHERE bibliographicrecordid=? AND agencyid=?";

    private static final String HISTORIC_METADATA = "SELECT created, modified, deleted, mimetype FROM records WHERE agencyid=? AND bibliographicrecordid=?"
                                                    + " UNION SELECT created, modified, deleted, mimetype FROM records_archive WHERE agencyid=? AND bibliographicrecordid=?"
                                                    + " ORDER BY modified DESC";
    private static final String HISTORIC_CONTENT = "SELECT content FROM records WHERE agencyid=? AND bibliographicrecordid=? AND modified=?"
                                                   + " UNION SELECT content FROM records_archive WHERE agencyid=? AND bibliographicrecordid=? AND modified=?";

    private static final String SELECT_RELATIONS = "SELECT refer_bibliographicrecordid, refer_agencyid FROM relations WHERE bibliographicrecordid=? AND agencyid=?";
    private static final String SELECT_RELATIONS_PARENTS = "SELECT refer_bibliographicrecordid, refer_agencyid FROM relations WHERE bibliographicrecordid=? AND agencyid=? AND refer_bibliographicrecordid <> bibliographicrecordid";
    private static final String SELECT_RELATIONS_CHILDREN = "SELECT bibliographicrecordid, agencyid FROM relations WHERE refer_bibliographicrecordid=? AND refer_agencyid=? AND refer_bibliographicrecordid <> bibliographicrecordid";
    private static final String SELECT_RELATIONS_SIBLINGS_FROM_ME = "SELECT bibliographicrecordid, agencyid FROM relations WHERE refer_bibliographicrecordid=? AND refer_agencyid=? AND refer_bibliographicrecordid = bibliographicrecordid";
    private static final String SELECT_RELATIONS_SIBLINGS_TO_ME = "SELECT refer_bibliographicrecordid, refer_agencyid FROM relations WHERE bibliographicrecordid=? AND agencyid=? AND refer_bibliographicrecordid = bibliographicrecordid";
    private static final String SELECT_ALL_AGENCIES_FOR_ID = "SELECT agencyid FROM records WHERE bibliographicrecordid=?";
    private static final String DELETE_RELATIONS = "DELETE FROM relations WHERE bibliographicrecordid=? AND agencyid=?";
    private static final String INSERT_RELATION = "INSERT INTO relations (bibliographicrecordid, agencyid, refer_bibliographicrecordid, refer_agencyid) VALUES(?, ?, ?, ?)";

    private static final String CALL_ENQUEUE_MIMETYPE = "{CALL enqueue(?, ?, ?, ?, ?, ?)}";
    private static final String CALL_DEQUEUE = "SELECT * FROM dequeue(?)";
    private static final String CALL_DEQUEUE_MULTI = "SELECT * FROM dequeue(?, ?)";
    private static final String QUEUE_ERROR = "INSERT INTO jobdiag(bibliographicrecordid, agencyid, worker, error, queued) VALUES(?, ?, ?, ?, ?)";

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
            try (PreparedStatement stmt = connection.prepareStatement(VALIDATE_SCHEMA)) {
                stmt.setInt(1, SCHEMA_VERSION);
                try (ResultSet resultSet = stmt.executeQuery()) {
                    if (resultSet.next()) {
                        String warning = resultSet.getString(1);
                        if (warning != null) {
                            log.warn(warning);
                        }
                        return;
                    }
                }
            }
        } catch (SQLException ex) {
            log.error("Validating schema", ex);
        }
        log.error("Incompatible database schema software=" + SCHEMA_VERSION);
        throw new RawRepoException("Incompatible database schema");
    }

    /**
     * Fetch a record from the database
     *
     * Create one if none exists in the database
     *
     * @param bibliographicRecordId String with record bibliographicRecordId
     * @param agencyId              agencyId number
     * @return fetched / new Record
     * @throws RawRepoException
     */
    @Override
    public Record fetchRecord(String bibliographicRecordId, int agencyId) throws RawRepoException {
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_RECORD)) {
            stmt.setString(1, bibliographicRecordId);
            stmt.setInt(2, agencyId);

            if (stmt.execute()) {
                try (ResultSet resultSet = stmt.executeQuery()) {
                    if (resultSet.next()) {
                        final boolean deleted = resultSet.getBoolean("DELETED");
                        final String mimeType = resultSet.getString("MIMETYPE");
                        final String base64Content = resultSet.getString("CONTENT");
                        byte[] content = base64Content == null ? null : DatatypeConverter.parseBase64Binary(base64Content);
                        Timestamp created = resultSet.getTimestamp("CREATED");
                        Timestamp modified = resultSet.getTimestamp("MODIFIED");
                        Record record = new RecordImpl(bibliographicRecordId, agencyId, deleted, mimeType, content, created, modified, false);

                        resultSet.close();
                        stmt.close();
                        return record;
                    }
                }
            }
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching record", ex);
        }
        return new RecordImpl(new RecordId(bibliographicRecordId, agencyId));
    }

    /**
     * Check for existence of a record
     *
     * @param bibliographicRecordId String with record bibliographicRecordId
     * @param agencyId              agencyId number
     * @return truth value for the existence of the record
     * @throws RawRepoException
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
     * @throws RawRepoException
     */
    @Override
    public boolean recordExistsMabyDeleted(String bibliographicRecordId, int agencyId) throws RawRepoException {
        Boolean recordDeleted = isRecordDeleted(bibliographicRecordId, agencyId);
        return recordDeleted != null;
    }

    @Override
    public List<RecordMetaDataHistory> getRecordHistory(String bibliographicRecordId, int agencyId) throws RawRepoException {
        try {
            ArrayList<RecordMetaDataHistory> ret = new ArrayList<>();
            try (PreparedStatement stmt = connection.prepareStatement(HISTORIC_METADATA);) {
                RecordId recordId = new RecordId(bibliographicRecordId, agencyId);
                stmt.setInt(1, agencyId);
                stmt.setString(2, bibliographicRecordId);
                stmt.setInt(3, agencyId);
                stmt.setString(4, bibliographicRecordId);
                try (ResultSet resultSet = stmt.executeQuery();) {
                    while (resultSet.next()) {
                        Timestamp created = resultSet.getTimestamp(1);
                        Timestamp modified = resultSet.getTimestamp(2);
                        boolean deleted = resultSet.getBoolean(3);
                        String mimeType = resultSet.getString(4);
                        ret.add(new RecordMetaDataHistory(recordId, deleted, mimeType, created, modified));
                    }
                }
            }
            return ret;
        } catch (Exception ex) {
            throw new RawRepoException("Error getting record history", ex);
        }
    }

    @Override
    public Record getHistoricRecord(RecordMetaDataHistory recordMetaData) throws RawRepoException {
        try {
            int agencyId = recordMetaData.getId().getAgencyId();
            String bibliographicRecordId = recordMetaData.getId().getBibliographicRecordId();
            Timestamp timestamp = recordMetaData.getTimestamp();
            try (PreparedStatement stmt = connection.prepareStatement(HISTORIC_CONTENT);) {
                stmt.setInt(1, agencyId);
                stmt.setString(2, bibliographicRecordId);
                stmt.setTimestamp(3, timestamp);
                stmt.setInt(4, agencyId);
                stmt.setString(5, bibliographicRecordId);
                stmt.setTimestamp(6, timestamp);
                try (ResultSet resultSet = stmt.executeQuery();) {
                    if (resultSet.next()) {
                        final String base64Content = resultSet.getString(1);
                        byte[] content = base64Content == null ? null : DatatypeConverter.parseBase64Binary(base64Content);
                        return new RecordImpl(bibliographicRecordId, agencyId, recordMetaData.isDeleted(),
                                              recordMetaData.getMimeType(), content,
                                              recordMetaData.getCreated(), recordMetaData.getModified(), false);
                    }
                }
            }
            throw new RawRepoExceptionRecordNotFound("Error getting record history");
        } catch (SQLException | RawRepoExceptionRecordNotFound ex) {
            throw new RawRepoException("Error getting record history", ex);
        }

    }

    /**
     * Purge a record from the database
     *
     * @param recordId complex key for record
     * @throws RawRepoException
     */
    @Override
    public void purgeRecord(RecordId recordId) throws RawRepoException {
        try (PreparedStatement stmt = connection.prepareStatement(PURGE_RECORD)) {
            stmt.setString(1, recordId.getBibliographicRecordId());
            stmt.setInt(2, recordId.getAgencyId());
            stmt.execute();
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error purging record", ex);
        }
    }
    private static final String LOG_DATABASE_ERROR = "Error accessing database";

    /**
     * Save a record to database after it has been modified
     *
     * Will try to update, otherwise insert
     *
     * @param record record to be saved
     * @throws RawRepoException
     */
    @Override
    public void saveRecord(Record record) throws RawRepoException {
        try (PreparedStatement stmt = connection.prepareStatement(UPDATE_RECORD)) {
            stmt.setBoolean(1, record.isDeleted());
            stmt.setString(2, record.getMimeType());
            stmt.setString(3, DatatypeConverter.printBase64Binary(record.getContent()));
            stmt.setTimestamp(4, new Timestamp(record.getModified().getTime()));
            stmt.setString(5, record.getId().getBibliographicRecordId());
            stmt.setInt(6, record.getId().getAgencyId());
            if (stmt.executeUpdate() > 0) {
                stmt.close();
                return;
            }
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error updating record", ex);
        }
        try (PreparedStatement stmt = connection.prepareStatement(INSERT_RECORD)) {
            stmt.setString(1, record.getId().getBibliographicRecordId());
            stmt.setInt(2, record.getId().getAgencyId());
            stmt.setBoolean(3, record.isDeleted());
            stmt.setString(4, record.getMimeType());
            stmt.setString(5, DatatypeConverter.printBase64Binary(record.getContent()));
            stmt.setTimestamp(6, new Timestamp(record.getCreated().getTime()));
            stmt.setTimestamp(7, new Timestamp(record.getModified().getTime()));
            stmt.execute();
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error saving record", ex);
        }
        if (record instanceof RecordImpl) {
            ( (RecordImpl) record ).original = false;
        }
    }

    @Override
    public String getMimeTypeOf(String bibliographicRecordId, int agencyId) throws RawRepoException {
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_MIMETYPE)) {
            stmt.setString(1, bibliographicRecordId);
            stmt.setInt(2, agencyId);
            if (stmt.execute()) {
                try (ResultSet resultSet = stmt.executeQuery()) {
                    if (resultSet.next()) {
                        return resultSet.getString(1);
                    }
                }
            }
            throw new RawRepoExceptionRecordNotFound("Trying to find mimetype");
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching mimetype", ex);
        }
    }

    private Boolean isRecordDeleted(String bibliographicRecordId, int agencyId) throws RawRepoException {
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_DELETED)) {
            stmt.setString(1, bibliographicRecordId);
            stmt.setInt(2, agencyId);
            if (stmt.execute()) {
                try (ResultSet resultSet = stmt.executeQuery()) {
                    if (resultSet.next()) {
                        return resultSet.getBoolean(1);
                    }
                }
            }
            return null;
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching deleted state", ex);
        }
    }

    /**
     * Get a collection of my "dependencies". All relations that
     * bibliographicRecordId have
     *
     * @param recordId complex key for a record
     * @return collection of recordids of whom bibliographicRecordId depends
     * @throws RawRepoException
     */
    @Override
    public Set<RecordId> getRelationsFrom(RecordId recordId) throws RawRepoException {
        Set<RecordId> collection = new HashSet<>();
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_RELATIONS)) {
            stmt.setString(1, recordId.getBibliographicRecordId());
            stmt.setInt(2, recordId.getAgencyId());
            if (stmt.execute()) {
                try (ResultSet resultSet = stmt.executeQuery()) {
                    while (resultSet.next()) {
                        collection.add(new RecordId(resultSet.getString(1), resultSet.getInt(2)));
                    }
                }
            }
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching relations", ex);
        }
        return collection;
    }

    /**
     * Delete all relations related to an bibliographicRecordId
     *
     * @param recordId complex key of bibliographicRecordId
     * @throws RawRepoException
     */
    @Override
    public void deleteRelationsFrom(RecordId recordId) throws RawRepoException {
        try (PreparedStatement stmt = connection.prepareStatement(DELETE_RELATIONS)) {
            stmt.setString(1, recordId.getBibliographicRecordId());
            stmt.setInt(2, recordId.getAgencyId());
            stmt.execute();
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error deleting relations", ex);
        }
    }

    /**
     * Clear all existing relations and set the new ones
     *
     * @param recordId recordid to update
     * @param refers   collection of recordids bibliographicRecordId depends on
     * @throws RawRepoException
     */
    @Override
    public void setRelationsFrom(RecordId recordId, Set<RecordId> refers) throws RawRepoException {
        ValidateRelations.validate(this, recordId, refers);

        deleteRelationsFrom(recordId);
        try (PreparedStatement stmt = connection.prepareStatement(INSERT_RELATION)) {
            stmt.setString(1, recordId.getBibliographicRecordId());
            stmt.setInt(2, recordId.getAgencyId());
            for (RecordId refer : refers) {
                stmt.setString(3, refer.getBibliographicRecordId());
                stmt.setInt(4, refer.getAgencyId());
                stmt.execute();
            }
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error setting relations", ex);
        }
    }

    /**
     * Get all records who has bibliographicRecordId as relation, but no
     * siblings
     *
     * @param recordId recordid to find relations to
     * @return collection of recordids that list bibliographicRecordId at
     *         relation
     * @throws RawRepoException
     */
    @Override
    public Set<RecordId> getRelationsChildren(RecordId recordId) throws RawRepoException {
        Set<RecordId> collection = new HashSet<>();
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_RELATIONS_CHILDREN)) {
            stmt.setString(1, recordId.getBibliographicRecordId());
            stmt.setInt(2, recordId.getAgencyId());
            if (stmt.execute()) {
                try (ResultSet resultSet = stmt.executeQuery()) {
                    while (resultSet.next()) {
                        collection.add(new RecordId(resultSet.getString(1), resultSet.getInt(2)));
                    }
                }
            }
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching relations", ex);
        }
        return collection;
    }

    /**
     * Get a collection of my "dependencies". All relations that
     * bibliographicRecordId have
     *
     * @param recordId complex key for a record
     * @return collection of recordids of whom bibliographicRecordId depends
     * @throws RawRepoException
     */
    @Override
    public Set<RecordId> getRelationsParents(RecordId recordId) throws RawRepoException {
        Set<RecordId> collection = new HashSet<>();
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_RELATIONS_PARENTS)) {
            stmt.setString(1, recordId.getBibliographicRecordId());
            stmt.setInt(2, recordId.getAgencyId());
            if (stmt.execute()) {
                try (ResultSet resultSet = stmt.executeQuery()) {
                    while (resultSet.next()) {
                        collection.add(new RecordId(resultSet.getString(1), resultSet.getInt(2)));
                    }
                }
            }
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching relations", ex);
        }
        return collection;
    }

    /**
     *
     * @param recordId
     * @return
     * @throws RawRepoException
     */
    @Override
    public Set<RecordId> getRelationsSiblingsToMe(RecordId recordId) throws RawRepoException {
        Set<RecordId> collection = new HashSet<>();
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_RELATIONS_SIBLINGS_FROM_ME)) {
            stmt.setString(1, recordId.getBibliographicRecordId());
            stmt.setInt(2, recordId.getAgencyId());
            if (stmt.execute()) {
                try (ResultSet resultSet = stmt.executeQuery()) {
                    while (resultSet.next()) {
                        collection.add(new RecordId(resultSet.getString(1), resultSet.getInt(2)));
                    }
                }
            }
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching relations", ex);
        }
        return collection;
    }

    /**
     *
     * @param recordId
     * @return
     * @throws RawRepoException
     */
    @Override
    public Set<RecordId> getRelationsSiblingsFromMe(RecordId recordId) throws RawRepoException {
        Set<RecordId> collection = new HashSet<>();
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_RELATIONS_SIBLINGS_TO_ME)) {
            stmt.setString(1, recordId.getBibliographicRecordId());
            stmt.setInt(2, recordId.getAgencyId());
            if (stmt.execute()) {
                try (ResultSet resultSet = stmt.executeQuery()) {
                    while (resultSet.next()) {
                        collection.add(new RecordId(resultSet.getString(1), resultSet.getInt(2)));
                    }
                }
            }
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching relations", ex);
        }
        return collection;
    }

    /**
     * Get all libraries that has local data to bibliographicRecordId
     *
     * @param bibliographicRecordId record bibliographicRecordId
     * @return Collection of libraries that has localdata for this record
     * @throws RawRepoException
     */
    @Override
    public Set<Integer> allAgenciesForBibliographicRecordId(String bibliographicRecordId) throws RawRepoException {
        Set<Integer> collection = new HashSet<>();
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_ALL_AGENCIES_FOR_ID)) {
            stmt.setString(1, bibliographicRecordId);
            if (stmt.execute()) {
                try (ResultSet resultSet = stmt.executeQuery()) {
                    while (resultSet.next()) {
                        collection.add(resultSet.getInt(1));
                    }
                }
            }
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error fetching relations", ex);
        }
        return collection;
    }

    /**
     * Put job(s) on the queue (in the database)
     *
     * @param job      job description
     * @param provider change initiator
     * @param mimeType
     * @param changed  is job for a record that has been changed
     * @param leaf     is this job for a tree leaf
     * @throws RawRepoException
     */
    @Override
    public void enqueue(RecordId job, String provider, String mimeType, boolean changed, boolean leaf) throws RawRepoException {
        try (CallableStatement stmt = connection.prepareCall(CALL_ENQUEUE_MIMETYPE)) {
            stmt.setString(1, job.getBibliographicRecordId());
            stmt.setInt(2, job.getAgencyId());
            stmt.setString(3, mimeType);
            stmt.setString(4, provider);
            stmt.setString(5, changed ? "Y" : "N");
            stmt.setString(6, leaf ? "Y" : "N");
            stmt.execute();
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error queueing job", ex);
        }
    }

    /**
     * Pull a job from the queue with rollback to savepoint capability
     *
     * @param worker name of worker that want's to take a job
     * @return job description
     * @throws RawRepoException
     */
    @Override
    public QueueJob dequeueWithSavepoint(String worker) throws RawRepoException {
        try (PreparedStatement begin = connection.prepareStatement("BEGIN")) {
            begin.execute();
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error dequeueing job", ex);
        }
        QueueJob result = dequeue(worker);
        try (PreparedStatement savepoint = connection.prepareStatement("SAVEPOINT DEQUEUED")) {
            savepoint.execute();
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error dequeueing job", ex);
        }
        return result;
    }

    /**
     * Pull a job from the queue
     *
     * @param worker name of worker that want's to take a job
     * @return job description
     * @throws RawRepoException
     */
    @Override
    public List<QueueJob> dequeue(String worker, int wanted) throws RawRepoException {
        List<QueueJob> result = new ArrayList<>();
        try (CallableStatement stmt = connection.prepareCall(CALL_DEQUEUE_MULTI)) {
            stmt.setString(1, worker);
            stmt.setInt(2, wanted);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    QueueJob job = new QueueJob(resultSet.getString("bibliographicrecordid"),
                                                resultSet.getInt("agencyid"),
                                                resultSet.getString("worker"),
                                                resultSet.getTimestamp("queued"));
                    result.add(job);
                }
                return result;
            }
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error dequeueing jobs", ex);
        }
    }

    /**
     * Pull a job from the queue
     *
     * @param worker name of worker that want's to take a job
     * @return job description
     * @throws RawRepoException
     */
    @Override
    public QueueJob dequeue(String worker) throws RawRepoException {
        try (CallableStatement stmt = connection.prepareCall(CALL_DEQUEUE)) {
            stmt.setString(1, worker);
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (resultSet.next()) {
                    return new QueueJob(resultSet.getString("bibliographicrecordid"),
                                        resultSet.getInt("agencyid"),
                                        resultSet.getString("worker"),
                                        resultSet.getTimestamp("queued"));
                }
                return null;
            }
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error dequeueing job", ex);
        }
    }

    /**
     * QueueJob has successfully been processed
     *
     * This is now the default when dequeuing
     *
     * @param queueJob job that has been processed
     * @throws RawRepoException
     */
    @Override
    @Deprecated
    public void queueSuccess(QueueJob queueJob) throws RawRepoException {
    }

    /**
     * QueueJob has failed, log to database
     *
     * @param queueJob job that failed
     * @param error    what happened (empty string not allowed)
     * @throws RawRepoException
     */
    @Override
    public void queueFail(QueueJob queueJob, String error) throws RawRepoException {
        if (error == null || error.equals("")) {
            throw new RawRepoException("Error cannot be empty in queueFail");
        }
        try (PreparedStatement stmt = connection.prepareStatement(QUEUE_ERROR)) {
            stmt.setString(1, queueJob.job.bibliographicRecordId);
            stmt.setInt(2, queueJob.job.agencyId);
            stmt.setString(3, queueJob.worker);
            stmt.setString(4, error);
            stmt.setTimestamp(5, queueJob.queued);
            stmt.executeUpdate();
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error reporting job status", ex);
        }
    }

    /**
     * QueueJob has failed
     *
     * @param queueJob job that failed
     * @param error    what happened (empty string not allowed)
     * @throws RawRepoException
     */
    @Override
    public void queueFailWithSavepoint(QueueJob queueJob, String error) throws RawRepoException {
        if (error == null || error.equals("")) {
            throw new RawRepoException("Error cannot be empty in queueFail");
        }
        try (PreparedStatement rollback = connection.prepareStatement("ROLLBACK TO DEQUEUED")) {
            rollback.execute();
        } catch (SQLException ex) {
            log.error(LOG_DATABASE_ERROR, ex);
            throw new RawRepoException("Error rolling back", ex);
        }
        queueFail(queueJob, error);
    }

}
