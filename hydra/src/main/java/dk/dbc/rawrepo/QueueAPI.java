/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.commons.jsonb.JSONBContext;
import dk.dbc.commons.jsonb.JSONBException;
import dk.dbc.rawrepo.common.ApplicationConstants;
import dk.dbc.rawrepo.common.EnvironmentVariables;
import dk.dbc.rawrepo.dao.HoldingsItemsConnector;
import dk.dbc.rawrepo.dao.OpenAgencyConnector;
import dk.dbc.rawrepo.dao.RawRepoConnector;
import dk.dbc.rawrepo.queue.AgencyAnalysis;
import dk.dbc.rawrepo.queue.QueueException;
import dk.dbc.rawrepo.queue.QueueJob;
import dk.dbc.rawrepo.queue.QueueProcessRequest;
import dk.dbc.rawrepo.queue.QueueProcessResponse;
import dk.dbc.rawrepo.queue.QueueProvider;
import dk.dbc.rawrepo.queue.QueueType;
import dk.dbc.rawrepo.queue.QueueValidateRequest;
import dk.dbc.rawrepo.queue.QueueValidateResponse;
import dk.dbc.rawrepo.timer.Stopwatch;
import dk.dbc.rawrepo.timer.StopwatchInterceptor;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.interceptor.Interceptors;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@Interceptors(StopwatchInterceptor.class)
@Stateless
@Path(ApplicationConstants.API_QUEUE)
public class QueueAPI {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(QueueAPI.class);

    private static final String MESSAGE_FAIL_NO_RECORDS = "Der blev ikke fundet nogen poster, så intet kan lægges på kø";
    private static final String MESSAGE_FAIL_INVALID_AGENCY_FORMAT = "Værdien '%s' har ikke et gyldigt format for et biblioteksnummer";
    private static final String MESSAGE_FAIL_INVAILD_AGENCY_ID = "Biblioteksnummeret '%s' tilhører ikke biblioteksgruppen %s";
    private static final String MESSAGE_FAIL_QUEUETYPE_NULL = "Der skal angives en køtype";
    private static final String MESSAGE_FAIL_QUEUETYPE = "Køtypen '%s' kunne ikke valideres";
    private static final String MESSAGE_FAIL_PROVIDER_NULL = "Der skal angives en provider";
    private static final String MESSAGE_FAIL_PROVIDER = "Provideren '%s' kunne ikke valideres";
    private static final String MESSAGE_FAIL_AGENCY_MISSING = "Der skal angives mindst ét biblioteksnummer";

    private static final String MESSAGE_FAIL_SESSION_ID_NULL = "Der skal være angivet et sessionId";
    private static final String MESSAGE_FAIL_SESSION_ID_NOT_FOUND = "SessionId '%s' blev ikke fundet";
    private static final String MESSAGE_FAIL_CHUNK_TOO_BIG = "Chunk index '%s' er for stort";
    private static final String MESSAGE_FAIL_CHUNK_NEGATIVE = "Chunk index må ikke være negativt";

    private static final Integer CHUNK_SIZE = 5000;

    @EJB
    OpenAgencyConnector openAgency;

    @EJB
    RawRepoConnector rawrepo;

    @EJB
    HoldingsItemsConnector holdingsItemsConnector;

    @EJB
    EnvironmentVariables variables;

    private final JSONBContext jsonbContext = new JSONBContext();

    // Not made private to facilitate easier testing
    final PassiveExpiringMap<String, QueueJob> jobCache = new PassiveExpiringMap<>(1000 * 60 * 60 * 8); // 8 hours

    @Stopwatch
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    @Path(ApplicationConstants.API_QUEUE_VALIDATE)
    public Response validate(String inputStr) {
        String res = "";
        final QueueValidateResponse response = new QueueValidateResponse();
        String sessionId = UUID.randomUUID().toString();

        try {
            try {
                final QueueValidateRequest queueValidateRequest = jsonbContext.unmarshall(inputStr, QueueValidateRequest.class);

                LOGGER.debug("Validate request with sessionId {}: {}", sessionId, queueValidateRequest);

                final QueueJob queueJob = prepareQueueJob(queueValidateRequest);
                loadRecordIdsForQueuing(queueJob);

                if (queueJob.getRecordIdList().size() == 0) {
                    throw new QueueException(MESSAGE_FAIL_NO_RECORDS);
                }

                final Map<Integer, Integer> agencySummary = new HashMap<>();

                for (RecordId recordId : queueJob.getRecordIdList()) {
                    Integer agencyId = recordId.getAgencyId();
                    Integer count = 1;

                    if (agencySummary.containsKey(agencyId)) {
                        count = agencySummary.get(agencyId);
                        count++;
                    }

                    agencySummary.put(agencyId, count);
                }

                LOGGER.debug("{} = {}", sessionId, queueJob);
                this.jobCache.put(sessionId, queueJob);

                agencySummary.forEach((key, value) -> response.getAgencyAnalysisList().add(new AgencyAnalysis(key, value)));

                response.setChunks(queueJob.getRecordIdList().size() / CHUNK_SIZE);
                response.setSessionId(sessionId);
                response.setValidated(true);

                res = jsonbContext.marshall(response);
            } catch (QueueException e) {
                response.setValidated(false);
                response.setMessage(e.getMessage());

                res = jsonbContext.marshall(response);
            }
            LOGGER.debug(response.toString());

            return Response.ok(res).build();
        } catch (Exception e) {
            LOGGER.error("Unexpected exception:", e);
            return Response.serverError().build();
        } finally {
            LOGGER.exit(res);
        }
    }

    private void loadRecordIdsForQueuing(QueueJob queueJob) throws Exception {
        Set<RecordId> holdingsItemsRecordIds;
        Set<RecordId> fbsRecordIds;
        Set<RecordId> recordMap = null;
        final QueueType queueType = queueJob.getQueueType();
        final Set<Integer> agencyList = queueJob.getAgencyIdList();
        final boolean includeDeleted = queueJob.isIncludeDeleted();

        switch (queueType.getKey()) {
            case QueueType.DBC_COMMON_ONLY:
                recordMap = rawrepo.getRecordsForDBC(agencyList, includeDeleted);
                break;
            case QueueType.FFU: // Intended fall through
            case QueueType.FBS_LOCAL:
                recordMap = rawrepo.getRecordsForAgencies(agencyList, includeDeleted);
                break;
            case QueueType.FBS_ENRICHMENT:
                recordMap = rawrepo.getEnrichmentForAgencies(agencyList, includeDeleted);
                break;
            case QueueType.FBS_HOLDINGS:
                holdingsItemsRecordIds = holdingsItemsConnector.getHoldingsRecords(agencyList);
                fbsRecordIds = rawrepo.getRecordsForAgencies(agencyList, includeDeleted);
                recordMap = convertNotExistingRecordIds(holdingsItemsRecordIds, fbsRecordIds);
                break;
            case QueueType.FBS_EVERYTHING:
                holdingsItemsRecordIds = holdingsItemsConnector.getHoldingsRecords(agencyList);
                fbsRecordIds = rawrepo.getRecordsForAgencies(agencyList, includeDeleted);
                recordMap = convertNotExistingRecordIds(holdingsItemsRecordIds, fbsRecordIds);
                // Rawrepo might contain records which the library doesn't have holdings on, so we have to add
                // the FBS records to the list as well
                recordMap.addAll(fbsRecordIds);
                break;
        }

        queueJob.setRecordIdList(recordMap);
    }

    @Stopwatch
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    @Path(ApplicationConstants.API_QUEUE_PROCESS)
    public Response process(String inputStr) {
        String res = "";
        final QueueProcessResponse response = new QueueProcessResponse();

        try {
            try {
                final QueueProcessRequest queueProcessRequest = jsonbContext.unmarshall(inputStr, QueueProcessRequest.class);

                LOGGER.debug(queueProcessRequest.toString());

                if (queueProcessRequest.getSessionId() == null || "null".equals(queueProcessRequest.getSessionId())) {
                    throw new QueueException(MESSAGE_FAIL_SESSION_ID_NULL);
                }

                if (jobCache.get(queueProcessRequest.getSessionId()) == null) {
                    throw new QueueException(String.format(MESSAGE_FAIL_SESSION_ID_NOT_FOUND, queueProcessRequest.getSessionId()));
                }

                final QueueJob queueJob = jobCache.get(queueProcessRequest.getSessionId());
                final int chunkIndex = queueProcessRequest.getChunkIndex();
                final String provider = queueJob.getProvider();
                final QueueType queueType = queueJob.getQueueType();
                final List<RecordId> recordIdList = new ArrayList<>(queueJob.getRecordIdList());

                if (chunkIndex > queueJob.getRecordIdList().size() / CHUNK_SIZE) {
                    throw new QueueException(String.format(MESSAGE_FAIL_CHUNK_TOO_BIG, chunkIndex));
                }

                if (chunkIndex < 0) {
                    throw new QueueException(MESSAGE_FAIL_CHUNK_NEGATIVE);
                }

                final int jobChunkStart = Math.max(0, chunkIndex * CHUNK_SIZE);
                final int jobChunkEnd = Math.min(recordIdList.size(), (chunkIndex + 1) * CHUNK_SIZE);
                final List<RecordId> chunk = recordIdList.subList(jobChunkStart, jobChunkEnd);

                LOGGER.info("Processing QueueJob with sessionId {} from {} to {} (total {} records)",
                        queueProcessRequest.getSessionId(),
                        jobChunkStart, jobChunkEnd,
                        queueJob.getRecordIdList().size());

                final List<String> bibliographicRecordIdList = new ArrayList<>();
                final List<Integer> agencyListBulk = new ArrayList<>();
                final List<String> providerList = new ArrayList<>();
                final List<Boolean> changedList = new ArrayList<>();
                final List<Boolean> leafList = new ArrayList<>();

                // This is a bit awkward construction, but necessary for setting parameters for a prepared statement
                for (RecordId recordId : chunk) {
                    bibliographicRecordIdList.add(recordId.getBibliographicRecordId());
                    agencyListBulk.add(recordId.getAgencyId());
                    providerList.add(provider);
                    changedList.add(queueType.isChanged());
                    leafList.add(queueType.isLeaf());
                }

                rawrepo.enqueueBulk(bibliographicRecordIdList, agencyListBulk, providerList, changedList, leafList);

                response.setValidated(true);

                res = jsonbContext.marshall(response);
            } catch (QueueException e) {
                response.setValidated(false);
                response.setMessage(e.getMessage());

                res = jsonbContext.marshall(response);
            }
            LOGGER.debug(response.toString());

            return Response.ok(res).build();
        } catch (Exception e) {
            LOGGER.error("Unexpected exception:", e);
            return Response.serverError().build();
        } finally {
            LOGGER.exit(res);
        }
    }

    private QueueJob prepareQueueJob(QueueValidateRequest queueValidateRequest) throws Exception {
        QueueJob queueJob = new QueueJob();

        try {
            final List<String> providerNames =
                    rawrepo.getProviders()
                            .stream()
                            .map(QueueProvider::getName)
                            .collect(Collectors.toList());

            // Convert list of QueueProvider to list of provider names
            final String provider = queueValidateRequest.getProvider();

            if (provider == null) {
                throw new QueueException(MESSAGE_FAIL_PROVIDER_NULL);
            }

            if (provider.isEmpty() || !providerNames.contains(provider)) {
                throw new QueueException(String.format(MESSAGE_FAIL_PROVIDER, provider));
            }
            queueJob.setProvider(provider);

            final String queueTypeString = queueValidateRequest.getQueueType();

            if (queueTypeString == null) {
                throw new QueueException(MESSAGE_FAIL_QUEUETYPE_NULL);
            }

            final QueueType queueType = QueueType.fromString(queueTypeString);
            if (queueType == null) {
                throw new QueueException(String.format(MESSAGE_FAIL_QUEUETYPE, queueValidateRequest.getQueueType()));
            }
            queueJob.setQueueType(queueType);

            queueJob.setIncludeDeleted(queueValidateRequest.isIncludeDeleted());

            String agencyString = queueValidateRequest.getAgencyText();
            if (agencyString == null) {
                throw new QueueException(MESSAGE_FAIL_AGENCY_MISSING);
            }
            // agencyText is a text field in which the user can write anything. Therefore we have to sanitize the
            // field first. Transform newline and space separation into comma separation for easier splitting
            agencyString = agencyString.replace("\n", ",");
            agencyString = agencyString.replace(" ", ",");
            // Remove eventual double or triple commas as a result of the replace
            while (agencyString.indexOf(",,") > 0) {
                agencyString = agencyString.replace(",,", ",");
            }
            final List<String> agencies = Arrays.asList(agencyString.split(","));
            final Set<String> allowedAgencies = openAgency.getLibrariesByCatalogingTemplateSet(queueType.getCatalogingTemplateSet());

            agencies.forEach(s -> s = s.trim());

            if (agencies.size() == 0) {
                throw new QueueException(MESSAGE_FAIL_AGENCY_MISSING);
            }

            final Set<Integer> agencyList = new HashSet<>();
            final Pattern p = Pattern.compile("(\\d{6})"); // Digit, length of 6
            for (String agency : agencies) {
                // This filtering could be done more efficiently with removeIf, however we want to check the format
                // so the user can be informed of invalid format
                if (!p.matcher(agency).find()) {
                    throw new QueueException(String.format(MESSAGE_FAIL_INVALID_AGENCY_FORMAT, agency));
                }
                if (!allowedAgencies.contains(agency)) {
                    throw new QueueException(String.format(MESSAGE_FAIL_INVAILD_AGENCY_ID, agency, queueType.getCatalogingTemplateSet()));
                }
                agencyList.add(Integer.parseInt(agency));
            }
            queueJob.setAgencyIdList(agencyList);

            return queueJob;
        } finally {
            LOGGER.exit(queueJob);
        }
    }

    /**
     * This function is necessary due to how Corepo works.
     * Corepo only contains real/existing record. However holdings items will often return ids of records
     * that doesn't actually exist. In those case we have to override the agencyId to be that of 191919 instead
     * so that Corepo receives the correct register
     *
     * @param holdingsItemsRecordIds Set of record ids from holdings items
     * @param fbsRecordIds           Set of record ids from rawrepo
     * @return Set of record ids
     */
    private Set<RecordId> convertNotExistingRecordIds(Set<RecordId> holdingsItemsRecordIds, Set<RecordId> fbsRecordIds) {
        final Set<RecordId> result = new HashSet<>();

        for (RecordId holdingsItemsRecordId : holdingsItemsRecordIds) {
            if (fbsRecordIds.contains(holdingsItemsRecordId)) {
                result.add(holdingsItemsRecordId);
            } else {
                result.add(new RecordId(holdingsItemsRecordId.bibliographicRecordId, 191919));
            }
        }

        return result;
    }

    @Stopwatch
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path(ApplicationConstants.API_QUEUE_PROVIDERS)
    public Response getProviders() {
        LOGGER.entry();
        String res = "";

        try {
            final List<QueueProvider> providers = rawrepo.getProviders();
            LOGGER.debug(providers.toString());

            res = jsonbContext.marshall(providers);
            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (Exception ex) {
            LOGGER.error("Exception during getProviderInfo", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.exit(res);
        }
    }

    @Stopwatch
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path(ApplicationConstants.API_QUEUE_TYPES)
    public Response getQueueTypes() {
        LOGGER.entry();

        String res = "";
        try {
            final List<QueueType> queueTypes = new ArrayList<>();
            queueTypes.add(QueueType.ffu());
            queueTypes.add(QueueType.fbsRawrepo());
            queueTypes.add(QueueType.fbsRawrepoEnrichment());
            queueTypes.add(QueueType.fbsHoldings());
            queueTypes.add(QueueType.fbsEverything());

            // Hack to only enable DBC queue type on basismig environment
            if (variables.getenv(ApplicationConstants.INSTANCE_NAME).toLowerCase().contains("basismig")) {
                queueTypes.add(QueueType.dbcCommon());
            }

            res = jsonbContext.marshall(queueTypes);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException e) {
            LOGGER.error("Unexpected exception:", e);
            return Response.serverError().build();
        } finally {
            LOGGER.exit(res);
        }
    }

}
