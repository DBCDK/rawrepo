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

import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public abstract class RawRepoDAO {

    private static final Logger log = LoggerFactory.getLogger(RawRepoDAO.class);

    AgencySearchOrder agencySearchOrder;

    /**
     * Load a class and instantiate it based on the driver name from the
     * supplied connection
     * <p>
     * USE:
     * {@link #newInstance(java.sql.Connection, dk.dbc.rawrepo.AgencySearchOrder)}
     *
     *
     * @param connection database connection
     * @return a RawRepoDAO for the connection
     * @throws RawRepoException
     */
    public static RawRepoDAO newInstance(Connection connection) throws RawRepoException {
        return newInstance(connection, new AgencySearchOrderFallback());
    }

    /**
     * Load a class and instantiate it based on the driver name from the
     * supplied connection
     *
     * @param connection        database connection
     * @param agencySearchOrder search order provider for an agency, only used
     *                          by {@link #fetchMergedRecord(java.lang.String, int, dk.dbc.marcxmerge.MarcXMerger)},
     *                          {@link #changedRecord(java.lang.String, dk.dbc.rawrepo.RecordId, java.lang.String)}
     *                          and
     *                          {@link #fetchRecordCollection(java.lang.String, int, dk.dbc.marcxmerge.MarcXMerger)}
     * @return a RawRepoDAO for the connection
     * @throws RawRepoException
     */
    public static RawRepoDAO newInstance(Connection connection, AgencySearchOrder agencySearchOrder) throws RawRepoException {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            String databaseProductName = metaData.getDatabaseProductName();
            String daoName = RawRepoDAO.class.getName();
            String className = daoName + databaseProductName + "Impl";
            Class<?> clazz = RawRepoDAO.class.getClassLoader().loadClass(className);
            if (!RawRepoDAO.class.isAssignableFrom(clazz)) {
                log.error("Class found by not an instance of RawRepoDAO");
                throw new RawRepoException("Unable to load driver");
            }
            Constructor<?> constructor = clazz.getConstructor(Connection.class);
            RawRepoDAO dao = (RawRepoDAO) constructor.newInstance(connection);
            dao.validateConnection();
            dao.agencySearchOrder = agencySearchOrder;
            return dao;
        } catch (SQLException | ClassNotFoundException | RawRepoException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            log.error("Caught exception tryini to instantiate dao", ex);
            throw new RawRepoException("Unable to load driver", ex);
        }
    }

    protected void validateConnection() throws RawRepoException {
    }

    /**
     * Fetch a record from the database
     *
     * Create one if none exists in the database
     * <p>
     * Remember, a record could exist, that is tagged deleted, call undelete()
     * If content is added.
     *
     * @param bibliographicRecordId String with record id
     * @param agencyId              library number
     * @return fetched / new Record
     * @throws RawRepoException
     */
    public abstract Record fetchRecord(String bibliographicRecordId, int agencyId) throws RawRepoException;

    /**
     * Find the mimetype of a record
     *
     * @param bibliographicRecordId
     * @param agencyId
     * @return
     * @throws RawRepoException
     */
    public abstract String getMimeTypeOf(String bibliographicRecordId, int agencyId) throws RawRepoException;

    /**
     * Check for existence of a record
     *
     * @param bibliographicRecordId String with record id
     * @param agencyId              library number
     * @return truth value for the existence of the record
     * @throws RawRepoException
     */
    public abstract boolean recordExists(String bibliographicRecordId, int agencyId) throws RawRepoException;

    /**
     * Check for existence of a record (possibly deleted)
     *
     * @param bibliographicRecordId String with record id
     * @param agencyId              library number
     * @return truth value for the existence of the record
     * @throws RawRepoException
     */
    public abstract boolean recordExistsMabyDeleted(String bibliographicRecordId, int agencyId) throws RawRepoException;

    /**
     * Get a collection of all the records, that are that are related to this
     *
     * @param bibliographicRecordId String with record id
     * @param agencyId              library number
     * @param merger
     * @return a collection of Record
     * @throws RawRepoException
     * @throws MarcXMergerException
     */
    public Map<String, Record> fetchRecordCollection(String bibliographicRecordId, int agencyId, MarcXMerger merger) throws RawRepoException, MarcXMergerException {
        HashMap<String, Record> ret = new HashMap<>();
        fetchRecordCollection(ret, bibliographicRecordId, agencyId, merger);
        return ret;
    }

    /**
     * Traverse references and fill into collection
     *
     * @param collection
     * @param bibliographicRecordId
     * @param agencyId
     * @param agencyIds
     * @param merger
     * @throws RawRepoException
     * @throws MarcXMergerException
     */
    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private void fetchRecordCollection(Map<String, Record> collection, String bibliographicRecordId, int agencyId, MarcXMerger merger) throws RawRepoException, MarcXMergerException {
        if (!collection.containsKey(bibliographicRecordId)) {
            Record record = fetchMergedRecord(bibliographicRecordId, agencyId, merger, false);
            collection.put(bibliographicRecordId, record);

            int mostCommonAgency = mostCommonAgencyForRecord(bibliographicRecordId, agencyId);
            Set<RecordId> parents = getRelationsParents(new RecordId(bibliographicRecordId, mostCommonAgency));
            for (RecordId parent : parents) {
                fetchRecordCollection(collection, parent.getBibliographicRecordId(), agencyId, merger);
            }
        }
    }

    /**
     * Fetch record for id, merging more common records with this
     *
     * @param bibliographicRecordId local id
     * @param originalAgencyId      least to most common
     * @param merger
     * @return Record merged
     * @throws RawRepoException     if there's a data error or record isn't
     *                              found
     * @throws MarcXMergerException if we can't merge record
     *
     * USE:
     * {@link #fetchMergedRecord(java.lang.String, int, dk.dbc.marcxmerge.MarcXMerger, boolean)}
     *
     */
    @Deprecated
    public Record fetchMergedRecord(String bibliographicRecordId, int originalAgencyId, MarcXMerger merger) throws RawRepoException, MarcXMergerException {
        return fetchMergedRecord(bibliographicRecordId, originalAgencyId, merger, false);
    }

    public int agencyFor(String bibliographicRecordId, int originalAgencyId, boolean fetchDeleted) throws RawRepoException {
        Set<Integer> allAgenciesWithRecord = allAgenciesForBibliographicRecordId(bibliographicRecordId);
        for (Integer agencyId : agencySearchOrder.getAgenciesFor(originalAgencyId)) {
            if (allAgenciesWithRecord.contains(agencyId)
                && ( fetchDeleted
                     ? recordExistsMabyDeleted(bibliographicRecordId, agencyId)
                     : recordExists(bibliographicRecordId, agencyId) )) {
                return agencyId;
            }
        }
        throw new RawRepoExceptionRecordNotFound("Cound not find base agency");
    }

    /**
     * Fetch record for id, merging more common records with this
     *
     * @param bibliographicRecordId local id
     * @param originalAgencyId      least to most common
     * @param merger
     * @param fetchDeleted          allow fetching of deleted records
     * @return Record merged
     * @throws RawRepoException     if there's a data error or record isn't
     *                              found
     * @throws MarcXMergerException if we can't merge record
     */
    public Record fetchMergedRecord(String bibliographicRecordId, int originalAgencyId, MarcXMerger merger, boolean fetchDeleted) throws RawRepoException, MarcXMergerException {
        int agencyId = agencyFor(bibliographicRecordId, originalAgencyId, fetchDeleted);
        LinkedList<Record> records = new LinkedList<>();
        for (;;) {
            Record record = fetchRecord(bibliographicRecordId, agencyId);
            records.addFirst(record);
            Set<RecordId> siblings = getRelationsSiblingsFromMe(record.getId());
            if (siblings.isEmpty()) {
                break;
            }
            agencyId = siblings.iterator().next().getAgencyId();
        }
        Iterator<Record> iterator = records.iterator();
        Record record = iterator.next();
        if (iterator.hasNext()) { // Record will be merged
            byte[] content = record.getContent();
            StringBuilder enrichmentTrail = new StringBuilder(record.getEnrichmentTrail());

            while (iterator.hasNext()) {
                Record next = iterator.next();
                log.debug("next = " + next.getId());
                if (!merger.canMerge(record.getMimeType(), next.getMimeType())) {
                    log.error("Cannot merge: " + record.getMimeType() + " and " + next.getMimeType());
                    throw new MarcXMergerException("Cannot merge enrichment");
                }

                content = merger.merge(content, next.getContent(), next.getId().getAgencyId() == originalAgencyId);
                enrichmentTrail.append(',').append(next.getId().getAgencyId());

                record = RecordImpl.Enriched(bibliographicRecordId, next.getId().getAgencyId(),
                                             record.getMimeType(), content,
                                             record.getCreated().after(next.getCreated()) ? record.getCreated() : next.getCreated(),
                                             record.getModified().after(next.getModified()) ? record.getModified() : next.getModified(),
                                             enrichmentTrail.toString());
            }
        }
        return record;
    }

    /**
     *
     * Create a list of all versions of a record
     *
     * @param bibliographicRecordId
     * @param agencyId
     * @return
     * @throws RawRepoException
     */
    public abstract List<RecordMetaDataHistory> getRecordHistory(String bibliographicRecordId, int agencyId) throws RawRepoException;

    /**
     * Fetch a specific version of a record
     *
     * @param recordMetaData
     * @return
     * @throws RawRepoException
     */
    public abstract Record getHistoricRecord(RecordMetaDataHistory recordMetaData) throws RawRepoException;

    @Deprecated
    /**
     * Delete a record from the database
     *
     * @param recordId complex key for record
     * @throws RawRepoException
     */
    public abstract void purgeRecord(RecordId recordId) throws RawRepoException;

    /**
     * Save a record to database after it has been modified
     *
     * Will try to update, otherwise insert
     *
     * @param record record to be saved
     * @throws RawRepoException
     */
    public abstract void saveRecord(Record record) throws RawRepoException;

    /**
     * Get a collection of my "dependencies".
     *
     * @param recordId complex key for a record
     * @return collection of recordids of whom id depends
     * @throws RawRepoException
     */
    public abstract Set<RecordId> getRelationsFrom(RecordId recordId) throws RawRepoException;

    /**
     * Delete all relations related to an id
     *
     * @param recordId complex key of id
     * @throws RawRepoException
     */
    public abstract void deleteRelationsFrom(RecordId recordId) throws RawRepoException;

    /**
     * Clear all existing relations and set the new ones
     *
     * @param recordId recordid to update
     * @param refers   collection of recordids id depends on
     * @throws RawRepoException
     */
    public abstract void setRelationsFrom(RecordId recordId, Set<RecordId> refers) throws RawRepoException;

    /**
     * Get all record relations from me with a different localid
     *
     * What "point upwards" from me
     *
     * @param recordId recordid to find relations to
     * @return collection of recordids that list id at relation
     * @throws RawRepoException
     */
    public abstract Set<RecordId> getRelationsParents(RecordId recordId) throws RawRepoException;

    /**
     * Get all record relations to me with a different localid
     *
     * What "points upwards" to me
     *
     * @param recordId recordid to find relations to
     * @return collection of recordids that list id at relation
     * @throws RawRepoException
     */
    public abstract Set<RecordId> getRelationsChildren(RecordId recordId) throws RawRepoException;

    /**
     * Get all records that points to me with same localid (less common
     * siblings)
     *
     * What "points sideways" to me
     *
     * @param recordId recordid to find relations to
     * @return collection of recordids that list id at relation
     * @throws RawRepoException
     */
    public abstract Set<RecordId> getRelationsSiblingsToMe(RecordId recordId) throws RawRepoException;

    /**
     * Get all records that points from me with same localid (more common
     * siblings)
     *
     * What "points sideways" from me
     *
     * @param recordId recordid to find relations to
     * @return collection of recordids that list id at relation
     * @throws RawRepoException
     */
    public abstract Set<RecordId> getRelationsSiblingsFromMe(RecordId recordId) throws RawRepoException;

    /**
     * Get all libraries that has id
     *
     *
     * @param bibliographicRecordId local id
     * @return Collection of libraries that has localid
     * @throws RawRepoException
     */
    public abstract Set<Integer> allAgenciesForBibliographicRecordId(String bibliographicRecordId) throws RawRepoException;

    /**
     * Traverse relations calling enqueue(...) to trigger manipulation of change
     *
     * @param provider         parameter to pass to enqueue(...)
     * @param recordId         the record that has been changed
     * @param fallbackMimetype Which mimetype to use when no mimetype can be
     *                         found (deleted)
     * @throws RawRepoException
     */
    public void changedRecord(String provider, RecordId recordId, String fallbackMimetype) throws RawRepoException {
        try {

            int mostCommonAgency = mostCommonAgencyForRecord(recordId.getBibliographicRecordId(), recordId.getAgencyId());
            // The mostCommonAgency, is the one that defines parent/child relationship
            Set<Integer> agencies = allParentAgenciesAffectedByChange(recordId);
            Set<RecordId> children = getRelationsChildren(new RecordId(recordId.getBibliographicRecordId(), mostCommonAgency));
            if (!agencies.isEmpty()) {
                String mimeType = getMimeTypeOf(recordId.getBibliographicRecordId(), mostCommonAgency);
                for (Integer agencyId : agencies) {
                    enqueue(new RecordId(recordId.getBibliographicRecordId(), agencyId), provider, mimeType, agencyId == recordId.getAgencyId(), children.isEmpty());
                }
            }
            for (RecordId child : children) {
                touchChildRecords(agencies, provider, child);
            }
        } catch (RawRepoExceptionRecordNotFound rawRepoExceptionRecordNotFound) {
            enqueue(recordId, provider, fallbackMimetype, true, true);
        }
    }

    /**
     * Traverse list of common libraries, to find least common version of record
     *
     * Then traverse siblings to find most common version of record, which is
     * provided by agencySearchOrder
     *
     * @param bibliographicRecordId
     * @return
     * @throws IllegalStateException
     * @throws RawRepoException
     */
    private int mostCommonAgencyForRecord(String bibliographicRecordId, int originalAgencyId) throws RawRepoException {
        for (Integer agencyId : agencySearchOrder.getAgenciesFor(originalAgencyId)) {
            if (recordExists(bibliographicRecordId, agencyId)) { // first available record
                Set<RecordId> siblings;
                // find most common through sibling relations
                // stops at localrecord or commonrecord
                while (!( siblings = getRelationsSiblingsFromMe(new RecordId(bibliographicRecordId, agencyId)) ).isEmpty()) {
                    if (siblings.size() != 1) {
                        throw new RawRepoException("record " + new RecordId(bibliographicRecordId, agencyId) + " points to multiple siblings");
                    }
                    RecordId sibling = siblings.iterator().next();
                    agencyId = sibling.getAgencyId();
                }
                return agencyId;
            }
        }
        log.error("Cannot locate agency for " + originalAgencyId + ":" + bibliographicRecordId);
        throw new RawRepoExceptionRecordNotFound("Cannot find record");
    }

    /**
     * Put job(s) on the queue (in the database)
     *
     * @param job      job description
     * @param provider change initiator
     * @param mimeType mimeType of the current record
     * @param changed  is job for a record that has been changed
     * @param leaf     is this job for a tree leaf
     * @throws RawRepoException
     */
    abstract void enqueue(RecordId job, String provider, String mimeType, boolean changed, boolean leaf) throws RawRepoException;

    /**
     * Pull a job from the queue
     *
     * Note: a queue should be dequeued either with this or
     * {@link #dequeue(java.lang.String, int) dequeue}, but not both. It could
     * break for long queues.
     *
     * @param worker name of worker that want's to take a job
     * @return job description
     * @throws RawRepoException
     */
    public abstract QueueJob dequeue(String worker) throws RawRepoException;

    /**
     * Pull jobs from the queue
     *
     * Note: a queue should be dequeued either with this or
     * {@link #dequeue(java.lang.String) dequeue}, but not both. It could break
     * for long queues.
     *
     * @param worker name of worker that want's to take a job
     * @param wanted number of jobs to dequeue
     * @return job description list
     * @throws RawRepoException
     */
    public abstract List<QueueJob> dequeue(String worker, int wanted) throws RawRepoException;

    /**
     * Pull a job from the queue with rollback to savepoint capability
     *
     * @param worker name of worker that want's to take a job
     * @return job description
     * @throws RawRepoException
     */
    public abstract QueueJob dequeueWithSavepoint(String worker) throws RawRepoException;

    /**
     * QueueJob has successfully been processed
     *
     * This is now the default when dequeuing
     *
     * @param queueJob job that has been processed
     * @throws RawRepoException
     */
    @Deprecated
    public abstract void queueSuccess(QueueJob queueJob) throws RawRepoException;

    /**
     * QueueJob has failed
     *
     * @param queueJob job that failed
     * @param error    what happened (empty string not allowed)
     * @throws RawRepoException
     */
    public abstract void queueFail(QueueJob queueJob, String error) throws RawRepoException;

    /**
     * QueueJob has failed
     *
     * @param queueJob job that failed
     * @param error    what happened (empty string not allowed)
     * @throws RawRepoException
     */
    public abstract void queueFailWithSavepoint(QueueJob queueJob, String error) throws RawRepoException;

    /**
     * Get a collection of Me and all siblings pointing towards me
     *
     * @param record
     * @return collection of library numbers
     * @throws RawRepoException
     */
    private Set<Integer> allSiblingAgenciesForRecord(RecordId record) throws RawRepoException {
        HashSet<Integer> ret = new HashSet<>();

        HashSet<RecordId> tmp = new HashSet<>();
        tmp.add(record);
        while (!tmp.isEmpty()) {
            Iterator<RecordId> iterator = tmp.iterator();
            RecordId next = iterator.next();
            iterator.remove();
            ret.add(next.getAgencyId());
            tmp.addAll(getRelationsSiblingsToMe(next));
        }
        return ret;
    }

    /**
     * When having a list of libraries and an id then return a new list (copy)
     * of all libraries, and all libraries that points towards something in this
     * collection
     *
     * @param agencyIds
     * @param bibliographicRecordId
     * @return collection of library numbers
     * @throws RawRepoException
     */
    private Set<Integer> expandSiblingsForId(Set<Integer> agencyIds, String bibliographicRecordId) throws RawRepoException {
        Set<Integer> ret = new HashSet<>();
        Set<Integer> tmp = new HashSet<>(agencyIds);
        while (!tmp.isEmpty()) {
            Iterator<Integer> iterator = tmp.iterator();
            int next = iterator.next();
            iterator.remove();
            ret.addAll(allSiblingAgenciesForRecord(new RecordId(bibliographicRecordId, next)));
        }
        return ret;
    }

    /**
     * Traverse up through parents, and add all affected libraries
     *
     * @param recordId
     * @return
     * @throws RawRepoException
     */
    private Set<Integer> allParentAgenciesAffectedByChange(RecordId recordId) throws RawRepoException {
        HashSet<Integer> ret = new HashSet<>();
        ret.addAll(allSiblingAgenciesForRecord(recordId));
        Set<RecordId> tmp = new HashSet<>();
        tmp.addAll(getRelationsParents(recordId));
        while (!tmp.isEmpty()) {
            Iterator<RecordId> iterator = tmp.iterator();
            RecordId next = iterator.next();
            iterator.remove();
            ret.addAll(allSiblingAgenciesForRecord(next));
            tmp.addAll(getRelationsParents(new RecordId(next.getBibliographicRecordId(), recordId.getAgencyId())));
        }
        return ret;
    }

    /**
     * Traverse down touching records for all affected libraries
     *
     * @param agencies
     * @param provider
     * @param recordId
     * @throws RawRepoException
     */
    private void touchChildRecords(Set<Integer> agencies, String provider, RecordId recordId) throws RawRepoException {
        Set<Integer> siblingAgencies = expandSiblingsForId(agencies, recordId.getBibliographicRecordId());
        Set<RecordId> children = getRelationsChildren(recordId);
        if (!siblingAgencies.isEmpty()) {
            int mostCommonAgency = mostCommonAgencyForRecord(recordId.getBibliographicRecordId(), siblingAgencies.iterator().next());
            String mimeType = getMimeTypeOf(recordId.getBibliographicRecordId(), mostCommonAgency);
            for (Integer agencyId : siblingAgencies) {
                enqueue(new RecordId(recordId.getBibliographicRecordId(), agencyId), provider, mimeType, false, children.isEmpty());
            }
        }
        for (RecordId child : children) {
            touchChildRecords(siblingAgencies, provider, child);
        }
    }

}
