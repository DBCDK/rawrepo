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
import java.util.ArrayList;
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
    public static final int COMMON_LIBRARY = 870970;

    /**
     * Load a class and instantiate it based on the driver name from the supplied connection
     *
     * @param connection database connection
     * @return a RawRepoDAO for the connection
     * @throws RawRepoException
     */
    public static RawRepoDAO newInstance(Connection connection) throws RawRepoException {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            String databaseProductName = metaData.getDatabaseProductName();
            String daoName = RawRepoDAO.class.getName();
            String className = daoName + databaseProductName + "Impl";
            Class<?> clazz = RawRepoDAO.class.getClassLoader().loadClass(className);
            if (!RawRepoDAO.class.isAssignableFrom(clazz)) {
                log.error("Claass found by not an instance of RawRepoDAO");
                throw new RawRepoException("Unable to load driver");
            }
            Constructor<?> constructor = clazz.getConstructor(Connection.class);
            RawRepoDAO dao = (RawRepoDAO) constructor.newInstance(connection);
            dao.validateConnection();
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
     *
     * @param bibliographicRecordId String with record id
     * @param agencyId library number
     * @return fetched / new Record
     * @throws RawRepoException
     */
    public abstract Record fetchRecord(String bibliographicRecordId, int agencyId) throws RawRepoException;

    /**
     * Check for existence of a record
     *
     * @param bibliographicRecordId String with record id
     * @param agencyId library number
     * @return truth value for the existence of the record
     * @throws RawRepoException
     */
    public abstract boolean recordExists(String bibliographicRecordId, int agencyId) throws RawRepoException;

    /**
     * Get a collection of all the records, that are that are related to this
     *
     * @param bibliographicRecordId String with record id
     * @param agencyId library number
     * @param merger
     * @return a collection of Record
     * @throws RawRepoException
     * @throws MarcXMergerException
     */
    public Map<String, Record> fetchRecordCollection(String bibliographicRecordId, int agencyId, MarcXMerger merger) throws RawRepoException, MarcXMergerException {
        HashMap<String, Record> ret = new HashMap<>();
        List<Integer> agencyIds = allCommonAgencies(agencyId);
        fetchRecordCollection(ret, bibliographicRecordId, agencyId, agencyIds, merger);
        return ret;
    }

    /**
     * USE fetchRecordCollection(String bibliographicRecordId, int agencyId, MarcXMerger merger)
     *
     * @param bibliographicRecordId
     * @param agencyId
     * @return
     * @throws RawRepoException
     * @throws MarcXMergerException
     */
    @Deprecated
    public Map<String, Record> fetchRecordCollection(String bibliographicRecordId, int agencyId) throws RawRepoException, MarcXMergerException {
        MarcXMerger merger = new MarcXMerger();
        return fetchRecordCollection(bibliographicRecordId, agencyId, merger);
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
    private void fetchRecordCollection(Map<String, Record> collection, String bibliographicRecordId, int agencyId, List<Integer> agencyIds, MarcXMerger merger) throws RawRepoException, MarcXMergerException {
        if (!collection.containsKey(bibliographicRecordId)) {
            ArrayList<Integer> allAgencies = new ArrayList<>(agencyIds);
            allAgencies.add(agencyId); // Ensure we get id:agency even if agency isn't in agencies list

            Record record = fetchMergedRecord(bibliographicRecordId, allAgencies, merger);
            collection.put(bibliographicRecordId, record);

            int mostCommonAgency = mostCommonAgencyForRecord(bibliographicRecordId, allAgencies);
            Set<RecordId> parents = getRelationsParents(new RecordId(bibliographicRecordId, mostCommonAgency));
            for (RecordId parent : parents) {
                fetchRecordCollection(collection, parent.getBibliographicRecordId(), parent.getAgencyId(), agencyIds, merger);
            }
        }
    }

    /**
     * Fetch record for id, merging more common records with this
     *
     * @param bibliographicRecordId local id
     * @param agencyIds least to most common
     * @param merger
     * @return Record merged
     * @throws RawRepoException if there's a data error or recoed isn't found
     * @throws MarcXMergerException if we can't merge record
     */
    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private Record fetchMergedRecord(String bibliographicRecordId, List<Integer> agencyIds, MarcXMerger merger) throws RawRepoException, MarcXMergerException {
        for (Integer agencyId : agencyIds) {
            if (recordExists(bibliographicRecordId, agencyId) /* && not deleted */) { // Least common agency for this record
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
                byte[] content = record.getContent();
                while (iterator.hasNext()) {
                    Record next = iterator.next();
                    content = merger.merge(content, next.getContent());
                    next.setContent(content);
                    if (record.getModified().after(next.getModified())) {
                        next.setModified(record.getModified());
                    }
                    if (record.getCreated().after(next.getCreated())) {
                        next.setCreated(record.getCreated());
                    }
                    record = next;
                }
                return record;
            }
        }
        throw new RawRepoExceptionRecordNotFound("Unable to find record");
    }

    /**
     * List of least to most common agency numbers for this agency
     *
     * @param recordId
     * @return
     * @throws RawRepoException
     */
    private List<Integer> allCommonAgencies(int agencyId) throws RawRepoException {
        List<Integer> agencyIds = new ArrayList<>();
        if (agencyId != COMMON_LIBRARY) {
            agencyIds.add(agencyId);
        }
        agencyIds.add(COMMON_LIBRARY);
        return agencyIds;
    }

    /**
     * Delete a record from the database
     *
     * Set content to null, remember to delete relations
     *
     * @param recordId complex key for record
     * @throws RawRepoException
     */
    public abstract void deleteRecord(RecordId recordId) throws RawRepoException;

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
     * @param refers collection of recordids id depends on
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
     * Get all records that points to me with same localid (less common siblings)
     *
     * What "points sideways" to me
     *
     * @param recordId recordid to find relations to
     * @return collection of recordids that list id at relation
     * @throws RawRepoException
     */
    public abstract Set<RecordId> getRelationsSiblingsToMe(RecordId recordId) throws RawRepoException;

    /**
     * Get all records that points from me with same localid (more common siblings)
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
     * USE allAgenciesForBibliographicRecordId
     *
     * @param bibliographicRecordId
     * @return
     * @throws RawRepoException
     * @deprecated
     */
    @Deprecated
    public Set<Integer> allLibrariesForId(String bibliographicRecordId) throws RawRepoException {
        return allAgenciesForBibliographicRecordId(bibliographicRecordId);
    }

    /**
     * Traverse relations calling enqueue(...) to trigger manipulation of change
     *
     * @param provider parameter to pass to enqueue(...)
     * @param recordId the record that has been changed
     * @throws RawRepoException
     */
    public void changedRecord(String provider, RecordId recordId) throws RawRepoException {
        int mostCommonLibrary = mostCommonAgencyForRecord(recordId.getBibliographicRecordId(), allCommonAgencies(recordId.getAgencyId()));
        // The mostCommonLibrary, is the one that defines parent/child relationship
        Set<Integer> libraries = allParentLibrariesAffectedByChange(recordId);
        Set<RecordId> children = getRelationsChildren(new RecordId(recordId.getBibliographicRecordId(), mostCommonLibrary));
        for (Integer agencyId : libraries) {
            enqueue(new RecordId(recordId.getBibliographicRecordId(), agencyId), provider, agencyId == recordId.getAgencyId(), children.isEmpty());
        }
        for (RecordId child : children) {
            touchChildRecords(libraries, provider, child);
        }

    }

    /**
     * Traverse list of common libraries, to find least common version of record
     *
     * Then traverse siblings to find most common version of record, which is either the COMMON_LIBRARY version or a LOCALRECORD
     *
     * @param bibliographicRecordId
     * @return
     * @throws IllegalStateException
     * @throws RawRepoException
     */
    private int mostCommonAgencyForRecord(String bibliographicRecordId, List<Integer> agencyIds) throws RawRepoException {
        for (Integer agencyId : agencyIds) {
            if (recordExists(bibliographicRecordId, agencyId)) { // first available record
                Set<RecordId> siblings;
                // find most common through sibling relations
                // stops at localrecord or commonrecord
                while (!(siblings = getRelationsSiblingsFromMe(new RecordId(bibliographicRecordId, agencyId))).isEmpty()) {
                    if (siblings.size() != 1) {
                        throw new RawRepoException("record " + new RecordId(bibliographicRecordId, agencyId) + " points to multiple siblings");
                    }
                    RecordId sibling = siblings.iterator().next();
                    agencyId = sibling.getAgencyId();
                }
                return agencyId;
            }
        }
        return COMMON_LIBRARY;
    }

    /**
     * Put job(s) on the queue (in the database)
     *
     * @param job job description
     * @param provider change initiator
     * @param changed is job for a record that has been changed
     * @param leaf is this job for a tree leaf
     * @throws RawRepoException
     */
    abstract void enqueue(RecordId job, String provider, boolean changed, boolean leaf) throws RawRepoException;

    /**
     * Pull a job from the queue
     *
     * @param worker name of worker that want's to take a job
     * @return job description
     * @throws RawRepoException
     */
    public abstract QueueJob dequeue(String worker) throws RawRepoException;

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
     * @param queueJob job that has been processed
     * @throws RawRepoException
     */
    public abstract void queueSuccess(QueueJob queueJob) throws RawRepoException;

    /**
     * QueueJob has failed
     *
     * @param queueJob job that failed
     * @param error what happened (empty string not allowed)
     * @throws RawRepoException
     */
    public abstract void queueFail(QueueJob queueJob, String error) throws RawRepoException;

    /**
     * QueueJob has failed
     *
     * @param queueJob job that failed
     * @param error what happened (empty string not allowed)
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
    private Set<Integer> allSiblingLibrariesForRecord(RecordId record) throws RawRepoException {
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
     * When having a list of libraries and an id then return a new list (copy) of all libraries, and all libraries that points towards something in this collection
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
            ret.addAll(allSiblingLibrariesForRecord(new RecordId(bibliographicRecordId, next)));
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
    private Set<Integer> allParentLibrariesAffectedByChange(RecordId recordId) throws RawRepoException {
        HashSet<Integer> ret = new HashSet<>();
        ret.addAll(allSiblingLibrariesForRecord(recordId));
        Set<RecordId> tmp = new HashSet<>();
        tmp.addAll(getRelationsParents(recordId));
        while (!tmp.isEmpty()) {
            Iterator<RecordId> iterator = tmp.iterator();
            RecordId next = iterator.next();
            iterator.remove();
            ret.addAll(allSiblingLibrariesForRecord(next));
            tmp.addAll(getRelationsParents(new RecordId(next.getBibliographicRecordId(), recordId.getAgencyId())));
        }
        return ret;
    }

    /**
     * Traverse down touching records for all affected libraries
     *
     * @param libraries
     * @param provider
     * @param recordId
     * @throws RawRepoException
     */
    private void touchChildRecords(Set<Integer> libraries, String provider, RecordId recordId) throws RawRepoException {
        Set<Integer> siblings = expandSiblingsForId(libraries, recordId.getBibliographicRecordId());
        Set<RecordId> children = getRelationsChildren(recordId);
        for (Integer agencyId : siblings) {
            enqueue(new RecordId(recordId.getBibliographicRecordId(), agencyId), provider, false, children.isEmpty());
        }
        for (RecordId child : children) {
            touchChildRecords(siblings, provider, child);
        }
    }

}
