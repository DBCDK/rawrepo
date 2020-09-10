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

import dk.dbc.marcrecord.ExpandCommonMarcRecord;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * @author DBC {@literal <dbc.dk>}
 */
public abstract class RawRepoDAO {

    private static final XLogger logger = XLoggerFactory.getXLogger(RawRepoDAO.class);

    RelationHintsOpenAgency relationHints;

    /**
     * Builder Pattern from RawRepoDAO
     */
    public static class Builder {

        private final Connection connection;
        private RelationHintsOpenAgency builderRelationHints;

        private Builder(Connection connection) {
            this.connection = connection;
            this.builderRelationHints = null;
        }

        /**
         * use
         * with a static service, to facilitate caching
         *
         * @param newRelationHints URL to openAgency service
         * @return self
         */
        public Builder relationHints(RelationHintsOpenAgency newRelationHints) {
            if (this.builderRelationHints != null) {
                throw new IllegalStateException("Cannot set relationHints again");
            }
            this.builderRelationHints = newRelationHints;
            return this;
        }

        /**
         * Construct a dao from the builder
         *
         * @return {@link RawRepoDAO} dao with default services, if none has
         * been provided.
         * @throws RawRepoException done at failure
         */
        public RawRepoDAO build() throws RawRepoException {
            try {
                RawRepoDAO dao = new RawRepoDAOPostgreSQLImpl(connection);
                dao.validateConnection();

                if (builderRelationHints == null) {
                    builderRelationHints = new RelationHintsOpenAgency(null);
                }
                dao.relationHints = builderRelationHints;

                return dao;
            } catch (RawRepoException | SecurityException | IllegalArgumentException ex) {
                logger.error("Caught exception trying to instantiate dao", ex);
                throw new RawRepoException("Unable to load driver", ex);
            }
        }
    }

    /**
     * Make a dao builder
     *
     * @param connection the database configuration
     * @return builder
     */
    public static Builder builder(Connection connection) {
        return new Builder(connection);
    }

    protected void validateConnection() throws RawRepoException {
    }

    /**
     * Fetch a record from the database
     * <p>
     * Create one if none exists in the database
     * <p>
     * Remember, a record could exist, that is tagged deleted, call undelete()
     * If content is added.
     *
     * @param bibliographicRecordId String with record id
     * @param agencyId              library number
     * @return fetched / new Record
     * @throws RawRepoException done at failure
     */
    public abstract Record fetchRecord(String bibliographicRecordId, int agencyId) throws RawRepoException;

    /**
     * Fetches a record from the cache table if the record is found.
     * <p>
     * In addition to the bibliographicRecordId and agencyId a cacheKey must be given as well.
     *
     * @param bibliographicRecordId String with record id
     * @param agencyId              library number
     * @param cacheKey              key defining the origin of the saved record
     * @return fetched record or null
     * @throws RawRepoException done at failure
     */
    public abstract Record fetchRecordCache(String bibliographicRecordId, int agencyId, String cacheKey) throws RawRepoException;

    /**
     * Find the mimetype of a record
     *
     * @param bibliographicRecordId record id to be examined
     * @param agencyId              the owner agency
     * @return the mimetype for a record
     * @throws RawRepoException done at failure
     */
    public abstract String getMimeTypeOf(String bibliographicRecordId, int agencyId) throws RawRepoException;

    public String getMimeTypeOfSafe(String bibliographicRecordId, int agencyId) throws RawRepoException {
        if (recordExistsMaybeDeleted(bibliographicRecordId, agencyId)) {
            return getMimeTypeOf(bibliographicRecordId, agencyId);
        }
        return MarcXChangeMimeType.UNKNOWN;
    }

    /**
     * Check for existence of a record
     *
     * @param bibliographicRecordId String with record id
     * @param agencyId              library number
     * @return truth value for the existence of the record
     * @throws RawRepoException done at failure
     */
    public abstract boolean recordExists(String bibliographicRecordId, int agencyId) throws RawRepoException;

    /**
     * Check for existence of a record (possibly deleted)
     *
     * @param bibliographicRecordId String with record id
     * @param agencyId              library number
     * @return truth value for the existence of the record
     * @throws RawRepoException done at failure
     */
    public abstract boolean recordExistsMaybeDeleted(String bibliographicRecordId, int agencyId) throws RawRepoException;

    /**
     * Get a collection of all the records, that are that are related to this
     *
     * @param bibliographicRecordId String with record id
     * @param agencyId              library number
     * @param merger                marc merger function
     * @return a collection of Record
     * @throws RawRepoException     done at failure
     * @throws MarcXMergerException done at failure
     */
    public Map<String, Record> fetchRecordCollection(String bibliographicRecordId, int agencyId, MarcXMerger merger) throws RawRepoException, MarcXMergerException {
        logger.info("fetchRecordCollection 1 for {}:{}", bibliographicRecordId, agencyId);
        HashMap<String, Record> ret = new HashMap<>();
        fetchRecordCollection(ret, bibliographicRecordId, agencyId, merger, true);
        return ret;
    }

    /**
     * Get a collection of all the records related to the input record
     *
     * @param bibliographicRecordId String with record id
     * @param agencyId              library number
     * @param merger                marc merger function
     * @param includeAutRecords     indicates if authority record should be included in the collection
     * @param keepAutFields         indicates if the *5 and *6 subfields should remain after authority expansion
     * @return a collection of Record
     * @throws RawRepoException     done at failure
     * @throws MarcXMergerException done at failure
     */
    public Map<String, Record> fetchRecordCollectionExpanded(String bibliographicRecordId, int agencyId, MarcXMerger merger, boolean includeAutRecords, boolean keepAutFields) throws RawRepoException, MarcXMergerException {
        logger.info("fetchRecordCollectionExpanded for {}:{}", bibliographicRecordId, agencyId);
        HashMap<String, Record> collection = new HashMap<>();
        fetchRecordCollectionExpanded(collection, bibliographicRecordId, agencyId, merger, includeAutRecords, keepAutFields);

        return collection;
    }

    /**
     * Get a collection of all the records related to the input record
     * - All applicable records are expanded with aut data
     * - Aut records are not included in the collection
     *
     * @param bibliographicRecordId String with record id
     * @param agencyId              library number
     * @param merger                marc merger function
     * @return a collection of Record
     * @throws RawRepoException     done at failure
     * @throws MarcXMergerException done at failure
     */
    public Map<String, Record> fetchRecordCollectionExpanded(String bibliographicRecordId, int agencyId, MarcXMerger merger) throws RawRepoException, MarcXMergerException {
        logger.info("fetchRecordCollectionExpanded for {}:{}", bibliographicRecordId, agencyId);
        HashMap<String, Record> collection = new HashMap<>();
        fetchRecordCollectionExpanded(collection, bibliographicRecordId, agencyId, merger, false, false);

        return collection;
    }

    private String generateExpandedRecordCacheKey(MarcXMerger merger, boolean includeAutRecords, boolean keepAutFields) {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPAND_");
        sb.append(merger.getName().toUpperCase()).append("_");
        sb.append(Boolean.toString(includeAutRecords)).append("_");
        sb.append(Boolean.toString(keepAutFields));

        return sb.toString();
    }

    private void fetchRecordCollectionExpanded(Map<String, Record> collection, String bibliographicRecordId, int agencyId, MarcXMerger merger, boolean includeAutRecords, boolean keepAutFields) throws
            RawRepoException, MarcXMergerException {
        if (!collection.containsKey(bibliographicRecordId)) {
            final String cacheKey = generateExpandedRecordCacheKey(merger, includeAutRecords, keepAutFields);
            Record record = fetchRecordCache(bibliographicRecordId, agencyId, cacheKey);
            if (record == null) {
                record = fetchMergedRecord(bibliographicRecordId, agencyId, merger, false);

                expandRecord(record, keepAutFields);

                saveRecordCache(record, cacheKey);
            }

            collection.put(bibliographicRecordId, record);

            int mostCommonAgency = findParentRelationAgency(bibliographicRecordId, agencyId);
            Set<RecordId> parents = getRelationsParents(new RecordId(bibliographicRecordId, mostCommonAgency));
            for (RecordId parent : parents) {
                // If this parent is an authority record and includeAut is false then skip parent
                if (870979 == parent.agencyId && !includeAutRecords) {
                    continue;
                }
                fetchRecordCollectionExpanded(collection, parent.getBibliographicRecordId(), agencyId, merger, includeAutRecords, keepAutFields);
            }
        }
    }

    private String generateMergeRecordCacheKey(MarcXMerger merger, boolean includeAutRecords) {
        StringBuilder sb = new StringBuilder();
        sb.append("MERGE_");
        sb.append(merger.getName().toUpperCase()).append("_");
        sb.append(Boolean.toString(includeAutRecords));

        return sb.toString();
    }

    /**
     * Traverse references and fill into collection
     * <p>
     * Can exclude authority records from the result set
     *
     * @param collection            A map to collect additional records in
     * @param bibliographicRecordId String with record id
     * @param agencyId              library number
     * @param merger                marc merger function
     * @param includeAut            true = include authority record in the collection, false = don't include authority records
     * @throws RawRepoException     done at failure
     * @throws MarcXMergerException done at failure
     */
    private void fetchRecordCollection(Map<String, Record> collection, String bibliographicRecordId, int agencyId, MarcXMerger merger, boolean includeAut) throws
            RawRepoException, MarcXMergerException {
        if (!collection.containsKey(bibliographicRecordId)) {
            final String cacheKey = generateMergeRecordCacheKey(merger, includeAut);
            Record record = fetchRecordCache(bibliographicRecordId, agencyId, cacheKey);
            if (record == null) {
                record = fetchMergedRecord(bibliographicRecordId, agencyId, merger, false);

                saveRecordCache(record, cacheKey);
            }
            collection.put(bibliographicRecordId, record);

            int mostCommonAgency = findParentRelationAgency(bibliographicRecordId, agencyId);
            Set<RecordId> parents = getRelationsParents(new RecordId(bibliographicRecordId, mostCommonAgency));
            for (RecordId parent : parents) {
                // If this parent is an authority record and includeAut is false then skip parent
                if (870979 == parent.agencyId && !includeAut) {
                    continue;
                }
                fetchRecordCollection(collection, parent.getBibliographicRecordId(), agencyId, merger, includeAut);
            }
        }
    }

    /**
     * Identify agency for a record, if agency doesn't have one self
     *
     * @param bibliographicRecordId record
     * @param originalAgencyId      agency requesting record
     * @param fetchDeleted          allow deleted records
     *                              first in openagency
     * @return agency that has the wanted record
     * @throws RawRepoException if no agency could be found
     */
    public int agencyFor(String bibliographicRecordId, int originalAgencyId, boolean fetchDeleted) throws RawRepoException {
        Set<Integer> allAgenciesWithRecord = allAgenciesForBibliographicRecordId(bibliographicRecordId);
        logger.debug("agencyFor record {}:{} has record for the following agencies: {}", bibliographicRecordId, originalAgencyId, allAgenciesWithRecord);

        List<Integer> agencyPriorityList = relationHints.getAgencyPriority(originalAgencyId);

        for (Integer agencyId : agencyPriorityList) {
            if (!allAgenciesWithRecord.contains(agencyId)) {
                continue;
            }
            if (fetchDeleted ?
                    recordExistsMaybeDeleted(bibliographicRecordId, agencyId) :
                    recordExists(bibliographicRecordId, agencyId)) {
                return agencyId;
            }
        }

        throw new RawRepoExceptionRecordNotFound("Could not find base agency for " + bibliographicRecordId + ":" + originalAgencyId +
                " No agency has this record that is located in openagency showOrder");
    }

    /**
     * Find agency for sibling relation
     *
     * @param bibliographicRecordId id of the record
     * @param originalAgencyId      the agency trying to make a sibling relation
     * @return agencyid of whom to make a sibling relation
     * @throws RawRepoException if no agency could be found for record
     */
    public int findSiblingRelationAgency(String bibliographicRecordId, int originalAgencyId) throws RawRepoException {
        if (!relationHints.usesCommonAgency(originalAgencyId)) {
            throw new RawRepoException("agency " + originalAgencyId + " does not use enrichments (Common agency)");
        } else {
            for (Integer agencyId : relationHints.get(originalAgencyId)) {
                if (recordExists(bibliographicRecordId, agencyId)) {
                    return agencyId;
                }
            }
        }
        throw new RawRepoExceptionRecordNotFound("Could not find (sibling) relation agency for " + bibliographicRecordId + " from " + originalAgencyId);
    }

    /**
     * Find agency for parent relation
     *
     * @param bibliographicRecordId id of the parent record
     * @param originalAgencyId      the agency trying to make a parent relation
     * @return agencyid of whom to make a parent relation
     * @throws RawRepoException if no agency could be found for record
     */
    public int findParentRelationAgency(String bibliographicRecordId, int originalAgencyId) throws RawRepoException {
        if (relationHints.usesCommonAgency(originalAgencyId)) {
            List<Integer> list = relationHints.get(originalAgencyId);
            if (!list.isEmpty()) {
                for (Integer agencyId : list) {
                    if (recordExists(bibliographicRecordId, agencyId)) {
                        return agencyId;
                    }
                }
            }
        }
        if (recordExists(bibliographicRecordId, originalAgencyId)) {
            return originalAgencyId;
        }
        throw new RawRepoExceptionRecordNotFound("Could not find (parent) relation agency for " + bibliographicRecordId + " from " + originalAgencyId);
    }

    /**
     * This function takes a Record and expands the content with aut data (if the record has any aut references)
     *
     * @param record       The record to expand
     * @param keepAutField Determines whether or not to keep the *5 and *6 subfields
     * @throws RawRepoException done at failure
     */
    public void expandRecord(Record record, boolean keepAutField) throws RawRepoException {
        final RecordId recordId = record.getId();
        final String bibliographicRecordId = recordId.getBibliographicRecordId();
        final int agencyId = recordId.getAgencyId();

        // Only get record collection if the record exist (there are no relations if the record doesn't exist or is deleted)
        if (recordExists(bibliographicRecordId, agencyId)) {
            logger.info("Record exists - checking if there is a 870970/870971 record");
            RecordId expandableRecordId = null;

            // Only these agencies can have authority parents
            final List<Integer> expandableAgencies = Arrays.asList(190002, 190004, 870970, 870971, 870974);

            if (expandableAgencies.contains(recordId.agencyId)) {
                expandableRecordId = recordId;
            } else {
                Set<RecordId> relationsSiblings = getRelationsSiblingsFromMe(recordId);
                for (int expandableAgencyId : expandableAgencies) {
                    RecordId potentialExpandableRecordId = new RecordId(bibliographicRecordId, expandableAgencyId);
                    if (relationsSiblings.contains(potentialExpandableRecordId)) {
                        expandableRecordId = potentialExpandableRecordId;
                        break;
                    }
                }
            }

            if (expandableRecordId != null) {
                logger.info("Expandable record found ({}) - continuing expanding", expandableRecordId.toString());

                final Set<RecordId> autParents = getRelationsParents(expandableRecordId);
                logger.info("Found {} parents to the expandable record", autParents.size());

                final Map<String, Record> autRecords = new HashMap<>();
                for (RecordId parentId : autParents) {
                    if ("870979".equals(Integer.toString(parentId.getAgencyId()))) {
                        logger.info("Found parent authority record: {}", parentId.toString());
                        autRecords.put(parentId.getBibliographicRecordId(), fetchRecord(parentId.getBibliographicRecordId(), parentId.getAgencyId()));
                    }
                }
                ExpandCommonMarcRecord.expandRecord(record, autRecords, keepAutField);
            }
        }
    }

    public Record fetchMergedRecordExpanded(String bibliographicRecordId, int originalAgencyId, MarcXMerger merger, boolean fetchDeleted, boolean keepAutFields)
            throws RawRepoException, MarcXMergerException {
        Record record = fetchMergedRecord(bibliographicRecordId, originalAgencyId, merger, fetchDeleted);

        expandRecord(record, keepAutFields);

        return record;
    }

    public Record fetchMergedRecordExpanded(String bibliographicRecordId, int originalAgencyId, MarcXMerger merger, boolean fetchDeleted)
            throws RawRepoException, MarcXMergerException {
        Record record = fetchMergedRecord(bibliographicRecordId, originalAgencyId, merger, fetchDeleted);

        expandRecord(record, false);

        return record;
    }

    public Record fetchRecordOrMergedRecord(String bibliographicRecordId, int originalAgencyId, MarcXMerger merger)
            throws dk.dbc.rawrepo.RawRepoException, dk.dbc.marcxmerge.MarcXMergerException {
        return fetchMergedRecord(bibliographicRecordId, originalAgencyId, merger, true);
    }

    /**
     * Fetch record for id, merging more common records with this
     *
     * @param bibliographicRecordId local id
     * @param originalAgencyId      least to most common
     * @param merger                marc merger function
     * @param fetchDeleted          allow fetching of deleted records
     * @return Record merged
     * @throws RawRepoException     if there's a data error or record isn't
     *                              found
     * @throws MarcXMergerException if we can't merge record
     */
    public Record fetchMergedRecord(String bibliographicRecordId, int originalAgencyId, MarcXMerger merger, boolean fetchDeleted)
            throws RawRepoException, MarcXMergerException {
        int agencyId = agencyFor(bibliographicRecordId, originalAgencyId, fetchDeleted);
        LinkedList<Record> records = new LinkedList<>();
        for (; ; ) {
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
                if (!merger.canMerge(record.getMimeType(), next.getMimeType())) {
                    logger.error("Cannot merge: " + record.getMimeType() + " and " + next.getMimeType());
                    throw new MarcXMergerException("Cannot merge enrichment");
                }

                content = merger.merge(content, next.getContent(), next.getId().getAgencyId() == originalAgencyId);
                enrichmentTrail.append(',').append(next.getId().getAgencyId());

                record = RecordImpl.enriched(bibliographicRecordId, next.getId().getAgencyId(),
                        merger.mergedMimetype(record.getMimeType(), next.getMimeType()), content,
                        record.getCreated().isAfter(next.getCreated()) ? record.getCreated() : next.getCreated(),
                        record.getModified().isAfter(next.getModified()) ? record.getModified() : next.getModified(),
                        record.getModified().isAfter(next.getModified()) ? record.getTrackingId() : next.getTrackingId(),
                        enrichmentTrail.toString());
            }
        }
        return record;
    }

    /**
     * Retrieve all trackingIds for a record since a specific time
     * <p>
     * This is useful for logging when multiple record updates result in one
     * queue entry
     *
     * @param bibliographicRecordId local id
     * @param agencyId              the agency
     * @param timestamp             start time
     * @return list of tracking id's
     * @throws RawRepoException done at failure
     */
    public abstract List<String> getTrackingIdsSince(String bibliographicRecordId, int agencyId, Timestamp timestamp) throws RawRepoException;

    /**
     * Create a list of all versions of a record
     *
     * @param bibliographicRecordId local id
     * @param agencyId              the agency
     * @return list of all records with given local id an agency
     * @throws RawRepoException done at failure
     */
    public abstract List<RecordMetaDataHistory> getRecordHistory(String bibliographicRecordId, int agencyId) throws RawRepoException;

    /**
     * Fetch a specific version of a record
     *
     * @param recordMetaData data identifying the record
     * @return the wanted record
     * @throws RawRepoException done at failure
     */
    public abstract Record getHistoricRecord(RecordMetaDataHistory recordMetaData) throws RawRepoException;

    /**
     * Save a record to database after it has been modified
     * <p>
     * Will try to update, otherwise insert
     *
     * @param record record to be saved
     * @throws RawRepoException done at failure
     */
    public abstract void saveRecord(Record record) throws RawRepoException;

    /**
     * Save a record to the cache table
     *
     * @param record   the record to cache
     * @param cacheKey the key which defines the parameter of the value saves
     * @throws RawRepoException done at failure
     */
    public abstract void saveRecordCache(Record record, String cacheKey) throws RawRepoException;

    /**
     * Get a collection of my "dependencies".
     *
     * @param recordId complex key for a record
     * @return collection of record ids of whom id depends
     * @throws RawRepoException done at failure
     */
    public abstract Set<RecordId> getRelationsFrom(RecordId recordId) throws RawRepoException;

    /**
     * Delete all relations related to an id
     *
     * @param recordId complex key of id
     * @throws RawRepoException done at failure
     */
    public abstract void deleteRelationsFrom(RecordId recordId) throws RawRepoException;

    /**
     * Clear all existing relations and set the new ones
     *
     * @param recordId recordid to update
     * @param refers   collection of record ids id depends on
     * @throws RawRepoException done at failure
     */
    public abstract void setRelationsFrom(RecordId recordId, Set<RecordId> refers) throws RawRepoException;

    /**
     * Get all record relations from me with a different localid
     * <p>
     * What "point upwards" from me
     *
     * @param recordId recordid to find relations to
     * @return collection of record ids that list id at relation
     * @throws RawRepoException done at failure
     */
    public abstract Set<RecordId> getRelationsParents(RecordId recordId) throws RawRepoException;

    /**
     * Get all record relations to me with a different localid
     * <p>
     * What "points upwards" to me
     *
     * @param recordId recordid to find relations to
     * @return collection of record ids that list id at relation
     * @throws RawRepoException done at failure
     */
    public abstract Set<RecordId> getRelationsChildren(RecordId recordId) throws RawRepoException;

    /**
     * Get all records that points to me with same localid (less common
     * siblings)
     * <p>
     * What "points sideways" to me
     *
     * @param recordId recordid to find relations to
     * @return collection of record ids that list id at relation
     * @throws RawRepoException done at failure
     */
    public abstract Set<RecordId> getRelationsSiblingsToMe(RecordId recordId) throws RawRepoException;

    /**
     * Get all records that points from me with same localid (more common
     * siblings)
     * <p>
     * What "points sideways" from me
     *
     * @param recordId recordid to find relations to
     * @return collection of record ids that list id at relation
     * @throws RawRepoException done at failure
     */
    public abstract Set<RecordId> getRelationsSiblingsFromMe(RecordId recordId) throws RawRepoException;

    /**
     * Get all libraries that has id
     *
     * @param bibliographicRecordId local id
     * @return Collection of libraries that has localid
     * @throws RawRepoException done at failure
     */
    public abstract Set<Integer> allAgenciesForBibliographicRecordId(String bibliographicRecordId) throws RawRepoException;

    public abstract Set<Integer> allAgenciesForBibliographicRecordIdSkipDeleted(String bibliographicRecordId) throws RawRepoException;

    /**
     * Put job(s) on the queue (in the database)
     *
     * @param job      job description
     * @param provider change initiator
     * @param changed  is job for a record that has been changed
     * @param leaf     is this job for a tree leaf
     * @throws RawRepoException done at failure
     */
    public abstract void enqueue(RecordId job, String provider, boolean changed, boolean leaf) throws RawRepoException;

    public abstract void enqueue(RecordId job, String provider, boolean changed, boolean leaf, int priority) throws RawRepoException;

    public abstract boolean checkProvider(String provider) throws RawRepoException;

    /**
     * Pull a job from the queue
     * <p>
     * Note: a queue should be dequeued either with this or
     * {@link #dequeue(java.lang.String, int) dequeue}, but not both. It could
     * break for long queues.
     *
     * @param worker name of worker that want's to take a job
     * @return job description
     * @throws RawRepoException done at failure
     */
    public abstract QueueJob dequeue(String worker) throws RawRepoException;

    /**
     * Pull jobs from the queue
     * <p>
     * Note: a queue should be dequeued either with this or
     * {@link #dequeue(java.lang.String) dequeue}, but not both. It could break
     * for long queues.
     *
     * @param worker name of worker that want's to take a job
     * @param wanted number of jobs to dequeue
     * @return job description list
     * @throws RawRepoException done at failure
     */
    public abstract List<QueueJob> dequeue(String worker, int wanted) throws RawRepoException;

    /**
     * QueueJob has failed
     *
     * @param queueJob job that failed
     * @param error    what happened (empty string not allowed)
     * @throws RawRepoException done at failure
     */
    public abstract void queueFail(QueueJob queueJob, String error) throws RawRepoException;

    /**
     * QueueJob has failed
     *
     * @param queueJob job that failed
     * @param error    what happened (empty string not allowed)
     * @throws RawRepoException done at failure
     */
    public abstract void queueFailWithSavepoint(QueueJob queueJob, String error) throws RawRepoException;

    /**
     * Traverse relations calling enqueue(...) to trigger manipulation of change
     * Uses default priority
     *
     * @param provider parameter to pass to enqueue(...)
     * @param recordId the record that has been changed
     * @throws RawRepoException done at failure
     */
    public void changedRecord(String provider, RecordId recordId) throws RawRepoException {
        changedRecord(provider, recordId, recordId.getAgencyId(), true, 1000);
    }

    /**
     * Traverse relations calling enqueue(...) to trigger manipulation of change
     * Priority can
     *
     * @param provider parameter to pass to enqueue(...)
     * @param recordId the record that has been changed
     * @param priority the priority of how fast the record should be dequeued - lower number = fast dequeue.
     * @throws RawRepoException done at failure
     *                          Default value is 1000
     */
    public void changedRecord(String provider, RecordId recordId, int priority) throws RawRepoException {
        changedRecord(provider, recordId, recordId.getAgencyId(), true, priority);
    }

    private void changedRecord(String provider, RecordId recordId, int originalAgencyId, boolean changed, int priority) throws RawRepoException {
        String bibliographicRecordId = recordId.getBibliographicRecordId();
        int agencyId = recordId.getAgencyId();
        if (recordExistsMaybeDeleted(bibliographicRecordId, agencyId)) {
            if (recordExists(bibliographicRecordId, agencyId)) {
                HashSet<Integer> agencyIds = findParentsSiblingsFilter(bibliographicRecordId, agencyId);
                changedRecord(provider, bibliographicRecordId, agencyIds, originalAgencyId, changed, priority);
            } else {
                enqueue(recordId, provider, true, true);
            }
        } else if (relationHints.usesCommonAgency(agencyId)) {
            logger.info("Queued non-existent record: " + agencyId + ":" + bibliographicRecordId);
            enqueue(recordId, provider, true, true);
        } else {
            throw new RawRepoExceptionRecordNotFound("Could not find record: " + agencyId + ":" + bibliographicRecordId);
        }
    }

    private void changedRecord(String provider, String bibliographicRecordId, Set<Integer> agencyIds, int originalAgencyId, boolean changed, int priority)
            throws RawRepoException {
        Set<Integer> agencies = new HashSet<>();
        for (Integer agencyId : agencyIds) {
            findMinorSiblingsAdd(agencies, bibliographicRecordId, agencyId, true);
        }
        Set<Integer> searchChildrenAgencies = new HashSet<>(agencies);
        for (Integer agency : agencies) {
            if (recordExists(bibliographicRecordId, agency)) {
                findMajorSiblings(searchChildrenAgencies, bibliographicRecordId, agency);
            } else {
                try {
                    searchChildrenAgencies.add(findSiblingRelationAgency(bibliographicRecordId, agency));
                } catch (RawRepoExceptionRecordNotFound ex) {
                    logger.warn("When trying to queue: " + bibliographicRecordId + " for " + agency + " could not find record");
                    logger.debug("relation hints showed no record");
                    logger.debug(ex.getMessage());
                }
            }
        }

        Set<RecordId> children = new HashSet<>();
        Set<RecordId> foreignChildren = new HashSet<>(); // Children with different agencyid
        Set<RecordId> minorChildren = new HashSet<>(); // Children that aren't

        for (Integer searchAgency : searchChildrenAgencies) {
            if (recordExists(bibliographicRecordId, searchAgency)) {
                for (RecordId recordId : getRelationsChildren(new RecordId(bibliographicRecordId, searchAgency))) {
                    if (recordId.getAgencyId() == searchAgency) {
                        if (agencies.contains(searchAgency)) {
                            children.add(recordId);
                        } else {
                            for (Integer agency : agencies) {
                                minorChildren.add(new RecordId(recordId.getBibliographicRecordId(), agency));
                            }
                        }
                    } else {
                        foreignChildren.add(recordId);
                    }
                }
            }
        }
        // Find all children only by major sibling search
        // Filter out those by direct reference... there should be none?
        Set<Integer> directChildAgencies = new HashSet<>();
        children.stream().map(RecordId::getAgencyId).forEach(directChildAgencies::add);
        foreignChildren.stream().map(RecordId::getAgencyId).forEach(directChildAgencies::add);
        minorChildren.stream()
                .filter(r -> !directChildAgencies.contains(r.getAgencyId()))
                .forEach(children::add);

        for (Integer agency : agencies) {
            RecordId recordId = new RecordId(bibliographicRecordId, agency);
            enqueue(recordId, provider,
                    changed && agency == originalAgencyId,
                    children.isEmpty(),
                    priority);
        }
        Set<String> bi = children.stream()
                .filter(r -> agencies.contains(r.getAgencyId()))
                .map(RecordId::getBibliographicRecordId)
                .collect(Collectors.toSet());
        for (String b : bi) {
            changedRecord(provider, b, agencies, -1, true, priority);
        }
        for (RecordId child : foreignChildren) {
            changedRecord(provider, child, child.getAgencyId(), false, priority);
        }
    }

    /**
     * Traverse sideways to major sibling and collect all agency ids.
     *
     * @param agencies              output collection
     * @param bibliographicRecordId id of record
     * @param agencyId              start agency
     * @throws RawRepoException iv record doesn't exist or relations has errors
     */
    private void findMajorSiblings(Set<Integer> agencies, String bibliographicRecordId, int agencyId) throws RawRepoException {
        Set<RecordId> siblings = getRelationsSiblingsFromMe(new RecordId(bibliographicRecordId, agencyId));
        for (RecordId sibling : siblings) {
            int siblingAgencyId = sibling.getAgencyId();
            if (!agencies.contains(siblingAgencyId)) {
                agencies.add(siblingAgencyId);
                findMajorSiblings(agencies, bibliographicRecordId, siblingAgencyId);
            }
        }
    }

    /**
     * Traverse to minor siblings, collecting agencyIds
     * <p>
     * If an agency is traversed that already is in agencies, then add all minor
     * siblings to this agency
     *
     * @param agencies              output of agencies seen
     * @param bibliographicRecordId record to start from
     * @param agencyId              agency to start from
     * @param add                   whether or not to add this
     * @throws RawRepoException if unable to find relations
     */
    private void findMinorSiblingsAdd(Set<Integer> agencies, String bibliographicRecordId, int agencyId, boolean add) throws RawRepoException {
        if (add) {
            agencies.add(agencyId);
        } else if (agencies.contains(agencyId)) {
            add = true;
        }
        Set<RecordId> relations = getRelationsSiblingsToMe(new RecordId(bibliographicRecordId, agencyId));
        for (RecordId relation : relations) {
            if (agencies.contains(relation.getAgencyId())) {
                add = true;
            }
            findMinorSiblingsAdd(agencies, bibliographicRecordId, relation.getAgencyId(), add);
        }
    }

    /**
     * Traverse major siblings, check for existence in loopTrack, adding loop
     * detection references
     *
     * @param agencies              output of agencies seen
     * @param bibliographicRecordId record to start from
     * @param agencyId              agency to start from
     * @throws RawRepoException if a loop occurs
     */
    private void findMajorSiblings(HashSet<Integer> agencies, String bibliographicRecordId, int agencyId) throws RawRepoException {
        RecordId recordId = new RecordId(bibliographicRecordId, agencyId);
        Set<RecordId> siblings = getRelationsSiblingsFromMe(recordId);
        for (RecordId sibling : siblings) {
            agencies.add(sibling.getAgencyId());
            findMajorSiblings(agencies, bibliographicRecordId, sibling.getAgencyId());
        }
    }

    /**
     * Traverse up in the structure collecting all major sibling agencies
     *
     * @param agencies              output of agencies seen
     * @param bibliographicRecordId record to start from
     * @param agencyId              agency to start from
     * @param add                   add minor siblings?
     * @throws RawRepoException if a loop occurs
     */
    private void findParentsSiblingsTraverse(HashSet<Integer> agencies, String bibliographicRecordId, int agencyId, boolean add) throws RawRepoException {
        RecordId recordId = new RecordId(bibliographicRecordId, agencyId);
        findMinorSiblingsAdd(agencies, bibliographicRecordId, agencyId, add);

        HashSet<Integer> major = new HashSet<>();
        findMajorSiblings(major, bibliographicRecordId, agencyId);
        for (Integer m : major) {
            findParentsSiblingsTraverse(agencies, bibliographicRecordId, m, false);
        }

        for (RecordId parent : getRelationsParents(recordId)) {
            if (canTraverseUp(recordId, parent)) {
                int parentAgency = parent.getAgencyId();
                findParentsSiblingsTraverse(agencies, parent.getBibliographicRecordId(), parentAgency, false);
            }
        }
    }

    private HashSet<Integer> findParentsSiblingsFilter(String bibliographicRecordId, int agencyId) throws RawRepoException {
        HashSet<Integer> agencies = new HashSet<>();
        findParentsSiblingsTraverse(agencies, bibliographicRecordId, agencyId, true);
        agencies.removeIf(a -> {
            try {
                return a != agencyId && recordExistsMaybeDeleted(bibliographicRecordId, a);
            } catch (RawRepoException ex) {
                logger.warn("Some SQLException converted to RawRepoException has been caught - why, oh why ? " + ex.toString());
            }
            return false;
        });
        return agencies;

    }

    /**
     * Can traverse up based upon mimetype
     *
     * @param recordId base
     * @param parentId target
     * @return mimetype combination allows for traverse
     * @throws RawRepoException done at failure
     */
    private boolean canTraverseUp(RecordId recordId, RecordId parentId) throws RawRepoException {
        String current = getMimeTypeOf(recordId.getBibliographicRecordId(), recordId.getAgencyId());
        String parent = getMimeTypeOf(parentId.getBibliographicRecordId(), parentId.getAgencyId());
        if (MarcXChangeMimeType.isArticle(current)) {
            return MarcXChangeMimeType.isArticle(parent);
        } else if (MarcXChangeMimeType.isMatVurd(current) || MarcXChangeMimeType.isLitAnalysis(current)) {
            return false;
        } else return MarcXChangeMimeType.isMarcXChange(current) ||
                MarcXChangeMimeType.isEnrichment(current);
    }
}
